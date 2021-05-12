package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.ido.robin.sstable.SegmentFile.SEGMENT_FILE_SUFFIX;

/**
 * @author Ido
 * @date 2019/1/11 14:32
 */
@Slf4j
public class FileManager {
    private FileSplitor fileSplitor;
    private Set<WeakReference<SegmentFileChangeListener>> listeners;
    private List<SegmentFile> segmentFiles;

    static final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    static final ReentrantReadWriteLock.WriteLock monitorWriteLock = reentrantLock.writeLock();
    static final ReentrantReadWriteLock.ReadLock monitorReadLock = reentrantLock.readLock();
    private Thread splitTask;
    private Thread autoFlushTask;
    private Thread expiredDataTask;
    private int splitTimeInterval = 120;
    private int autoFlushTimeInterval = 120;
    private int deleteExpiredDataTimeInterval = 60;
    private int autoFlushCmdsSize = 100;


    public FileManager(FileSplitor fileSplitor) {
        this.fileSplitor = fileSplitor;
    }

    public FileManager() {
        this.fileSplitor = new LocalFileSystemSplitor();
    }

    public int getSplitTimeInterval() {
        return splitTimeInterval;
    }

    public void setSplitTimeInterval(int splitTimeInterval) {
        this.splitTimeInterval = splitTimeInterval;
    }

    public int getAutoFlushTimeInterval() {
        return autoFlushTimeInterval;
    }

    public void setAutoFlushTimeInterval(int autoFlushTimeInterval) {
        this.autoFlushTimeInterval = autoFlushTimeInterval;
    }

    public int getDeleteExpiredDataTimeInterval() {
        return deleteExpiredDataTimeInterval;
    }

    public void setDeleteExpiredDataTimeInterval(int deleteExpiredDataTimeInterval) {
        this.deleteExpiredDataTimeInterval = deleteExpiredDataTimeInterval;
    }

    public int getAutoFlushCmdsSize() {
        return autoFlushCmdsSize;
    }

    public void setAutoFlushCmdsSize(int autoFlushCmdsSize) {
        this.autoFlushCmdsSize = autoFlushCmdsSize;
    }

    Set<WeakReference<SegmentFileChangeListener>> getListeners() {
        return listeners;
    }


    /**
     * 监听当前目录下的所有segment 文件,自动拆分
     *
     * @param path
     * @param period
     * @param timeUnit
     */
    public void setupAutoSplitTask(String path, int period, TimeUnit timeUnit) {
        splitTask = new Thread(new AutoSplitFileTask(path, period, timeUnit));
        splitTask.setDaemon(true);
        splitTask.start();

    }


    public void setupAutoFlush(String path, int period, TimeUnit timeUnit) {
        autoFlushTask = new Thread(new AutoFlushTask(path, period, timeUnit));
        autoFlushTask.setDaemon(true);
        autoFlushTask.start();
    }

    public void setupExpiredDataTask(String path, int period, TimeUnit timeUnit) {
        this.expiredDataTask = new Thread(new AutoDeleteExpiredDataTask(path, period, timeUnit));
        this.expiredDataTask.setDaemon(true);
        this.expiredDataTask.start();
    }


    public void shutdown() {
        if (autoFlushTask != null) {
            autoFlushTask.interrupt();
        }
        if (splitTask != null) {
            splitTask.interrupt();
        }
        if (expiredDataTask != null) {
            expiredDataTask.interrupt();
        }
    }

    public interface SegmentFileChangeListener {
        void onChange(List<SegmentFile> segmentFiles, List<String> changedSgFiles);
    }

    public void addSegmentFileChangeListener(SegmentFileChangeListener listener) {
        if (listeners == null) {
            listeners = new HashSet<>();
        }
        WeakReference<SegmentFileChangeListener> l = new WeakReference<>(listener);
        listeners.add(l);
    }

    public List<SegmentFile> getSegmentFiles() {
        return segmentFiles;
    }

    public FileManager setSegmentFiles(List<SegmentFile> segmentFiles) {
        this.segmentFiles = segmentFiles;
        return this;
    }

    private void notifySegmentFileChange(List<SegmentFile> segmentFiles, List<String> changedSgFiles) {
        listeners = listeners.stream().filter(r->r.get()!=null).collect(Collectors.toSet());
        listeners.stream().forEach(l -> {
            SegmentFileChangeListener listener = l.get();
            if (listener != null) {
                listener.onChange(segmentFiles, changedSgFiles);
            }

        });

    }


    public static List<SegmentFile> listSegFiles(String path) {
        try {
            return Files.list(Paths.get(path))
                    .filter(p -> p.toFile().getName().endsWith(SEGMENT_FILE_SUFFIX))
                    .map(Path::toFile)

                    .map(f -> {
                        try {
                            return new SegmentFile(f);
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return new ArrayList<>(0);
    }

    /**
     * 监听当前目录下的所有segment 文件
     *
     * @param path
     */
    public void setupAutoSplitTask(String path) {
        setupAutoSplitTask(path, splitTimeInterval, TimeUnit.SECONDS);
    }

    public void setupAutoFlush(String path) {
        setupAutoFlush(path, autoFlushTimeInterval, TimeUnit.SECONDS);
    }

    public void setupExpiredDataTask(String path) {
        setupExpiredDataTask(path, deleteExpiredDataTimeInterval, TimeUnit.SECONDS);
    }


    abstract class AbstractFileTask implements Runnable {
        String path;
        int period;
        TimeUnit timeUnit;

        public AbstractFileTask(String path, int period, TimeUnit timeUnit) {
            this.path = path;
            this.period = period;
            this.timeUnit = timeUnit;
        }

        abstract protected void innerRun();

        @Override
        public void run() {
            while (true) {
                try {
                    timeUnit.sleep(period);
                } catch (InterruptedException e) {
                    log.info("thread interrupted");
                    break;
                }
                try {
                    monitorWriteLock.lock();
                    log.debug("acquiring monitorWriteLock ");
                    innerRun();
                } finally {
                    log.debug("release monitorWriteLock ");
                    monitorWriteLock.unlock();
                }
            }


        }
    }

    class AutoSplitFileTask extends AbstractFileTask {

        AutoSplitFileTask(String path, int period, TimeUnit timeUnit) {
            super(path, period, timeUnit);
        }

        @Override
        protected void innerRun() {
            List<SegmentFile> segmentFiles = fileSplitor.listToBeSplittedSgFile(path);
            List<String> changedSgFiles;
            if (!segmentFiles.isEmpty()) {
                changedSgFiles = new ArrayList<>();
                segmentFiles.forEach(r -> {
                    List<String> generateFiles = fileSplitor.split(r);
                    changedSgFiles.addAll(generateFiles);
                    if (generateFiles.size() > 1) {
                        log.info("file [{}] auto split into [{}]", r.getOriginalFileName(), generateFiles.toString());
                    }

                });

                // 通知SSTable 更新 segment file list
                notifySegmentFileChange(listSegFiles(path), changedSgFiles);


            }
        }
    }


    class AutoFlushTask extends AbstractFileTask {
        int size = autoFlushCmdsSize;

        AutoFlushTask(String path, int period, TimeUnit timeUnit) {
            super(path, period, timeUnit);
        }

        @Override
        protected void innerRun() {
            segmentFiles.forEach(sg -> {
                //如果内存中待持久化的命令 长度大于指定值，自动flush
                if (sg.getCmdsSize() >= size || sg.getToAddBlockList().size() >= size || sg.getToRemoveBlockList().size() >= size) {
                    log.info("auto flush {} cmds", sg.getCmdsSize());
                    log.info("auto flush {} ToAddBlockList", sg.getToAddBlockList().size());
                    log.info("auto flush {} ToRemoveBlockList", sg.getToRemoveBlockList().size());
                    sg.flush();
                }
            });
        }
    }


    class AutoDeleteExpiredDataTask extends AbstractFileTask {

        AutoDeleteExpiredDataTask(String path, int period, TimeUnit timeUnit) {
            super(path, period, timeUnit);
        }

        @Override
        protected void innerRun() {
            segmentFiles.forEach(sg -> {
                List<Block> blocks = sg.getEmptyValueBlockList();
                blocks.forEach(b -> {
                    if (b.isExpired()) {
                        sg.remove(b.getKey());
                    }
                });
                sg.flush();
            });
        }
    }


}
