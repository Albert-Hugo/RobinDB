package com.ido.robin.sstable.wal;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2021/1/6 14:47
 */
@Slf4j
public class WalManager {
    private String path;
    private SequenceManager sequenceManager = new SequenceManager();
    private List<WalDataFile> walDataFiles = new ArrayList<>();

    static final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    //todo set up lock
    static final ReentrantReadWriteLock.WriteLock monitorWriteLock = reentrantLock.writeLock();
    static final ReentrantReadWriteLock.ReadLock monitorReadLock = reentrantLock.readLock();
    private Thread autoSnapshotTask;

    /**
     * snapshot 记录点
     */
    private long snapshotCheckPoint = 0;
    /**
     * 回复之后的最后一条记录ID
     */
    private long recoverySavePoint = 0;

    public void markRecoverySavePoint(long lastEntryId) {
        this.recoverySavePoint = lastEntryId;
    }

    private class AutoSnapshotTask extends AbstractFileTask {

        public AutoSnapshotTask(String path, int period, TimeUnit timeUnit) {
            super(path, period, timeUnit);
        }

        @Override
        protected void innerRun() {
            try {
                takeSnapshot();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
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


    /**
     * 清除废旧的log
     * 将当前所有的日志遍历出来，删除 recoverySavePoint  之前的所有无用数据
     */
    public void cleanAfterRecovery() {
        //等 recovery 之后，把恢复之后最新的entryId 记录下来，然后清除掉已经无用的内容
        List<WalLogData> list = listWalDatas();
        List<WalLogData> toSave = new ArrayList<>();
        for (WalLogData d : list) {
            if (d.getSequence() > this.recoverySavePoint) {
                toSave.add(d);
            }
        }

        try {
            WalDataFile newDataFile = new WalDataFile(newWalDataFileName());
            //清除掉所有旧的废弃日志
            clearOldFile(newDataFile);
            newDataFile.batchAppend(toSave);
            synchronized (this.walDataFiles){
                this.walDataFiles.add(newDataFile);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }


    }

    private void clearOldFile(WalDataFile newDataFile) throws IOException {
        synchronized (this.walDataFiles){

            for (WalDataFile walDataFile : this.walDataFiles) {
                walDataFile.close();
            }
            this.walDataFiles.clear();
            this.walDataFiles.add(newDataFile);
            List<File> oldFiles = Files.list(Paths.get(this.path))
                    .filter(p -> p.toFile().getName().endsWith(WalDataFile.WAL_FILE_PREFIX))
                    .map(Path::toFile)
                    .filter(Objects::nonNull)
                    .filter(f -> !f.getName().equals(newDataFile.getFileName()))
                    .collect(Collectors.toList());

            for (File f : oldFiles) {
                try {
                    java.nio.file.Files.delete(f.toPath());
                } catch (IOException ex) {
                    log.error(ex.getMessage(), ex);
                }
            }
        }
    }

    private String newWalDataFileName() {
        return this.path + sequenceManager.next() + WalDataFile.WAL_FILE_PREFIX;
    }

    /**
     * 获取快照，将当前所有的log 合并，只获取针对同一个Key 最新的command
     */
    public void takeSnapshot() throws IOException {
        //从wal 文件中遍历，出所有命令，然后合并成新的snapshot
        List<WalLogData> logData = listWalDatas((WalLogData a1, WalLogData a2) -> {
            if(a1 == null && a2 == null){
                return 0;
            }
            if(a1 == null){
                return 1;
            }

            if(a2 == null){
                return -1;
            }
            if(a2.getSequence() - a1.getSequence() >0 ){
                return 1;
            }else if(a2.getSequence() - a1.getSequence() <0){
                return -1;
            }

            return 0;

        });

        Map<String, SnapshotLogData> snapshotLogData = new HashMap<>();
        //对于同一个key ，只需要获取最新的命令即可
        for (WalLogData d : logData) {
            if (snapshotLogData.get(new String(d.getKey())) == null) {
                snapshotLogData.put(new String(d.getKey()), new SnapshotLogData(d.getCmd(), d.getKey(), d.getVal(), d.getSequence()));
            }
        }
        List<WalLogData> snapshotLogs = snapshotLogData.entrySet().stream().map(en -> {
            SnapshotLogData sd = en.getValue();
            return new WalLogData(sd.cmd, sd.key, sd.val, sd.sequence);
        }).collect(Collectors.toList());

        //create a new wal file to save snapshot and delete all old file
        String fileName = newWalDataFileName();
        WalDataFile walDataFile = new WalDataFile(fileName);
        walDataFile.batchAppend(snapshotLogs);
        clearOldFile(walDataFile);

        if (!snapshotLogs.isEmpty()) {
            long newestSequence = snapshotLogs.get(0).getSequence();
            this.snapshotCheckPoint = newestSequence;
            log.info("task a snapshot complete");
        }

    }


    private static class SnapshotLogData {
        Cmd cmd;
        byte[] key;
        byte[] val;
        long sequence;

        public SnapshotLogData(Cmd cmd, byte[] key, byte[] val, long sequence) {
            this.cmd = cmd;
            this.key = key;
            this.val = val;
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SnapshotLogData)) return false;

            SnapshotLogData that = (SnapshotLogData) o;

            if (cmd != that.cmd) return false;
            return Arrays.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            int result = cmd != null ? cmd.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(key);
            return result;
        }
    }

    public WalManager(String path) {
        this.path = path;
        synchronized (this.walDataFiles){

            try {
                this.walDataFiles = Files.list(Paths.get(this.path))
                        .filter(p -> p.toFile().getName().endsWith(WalDataFile.WAL_FILE_PREFIX))
                        .map(Path::toFile)
                        .map(f -> {
                            try {
                                return new WalDataFile(f);
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

            if (walDataFiles.isEmpty()) {
                try {
                    walDataFiles.add(new WalDataFile(newWalDataFileName()));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }


            //set up auto snapshot task
            autoSnapshotTask = new Thread(new AutoSnapshotTask(this.path, 60, TimeUnit.SECONDS));
            autoSnapshotTask.setDaemon(true);
            autoSnapshotTask.start();
        }


        log.debug(walDataFiles.size() + "");
    }

    public void append(Cmd cmd, String key, String val) {
        try {
            getCurrentWalFile().append(new WalLogData(cmd, key.getBytes(), val != null ? val.getBytes() : null, sequenceManager.next()));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 获取当前在写的 wal file
     *
     * @return
     */
    private WalDataFile getCurrentWalFile() {
        synchronized (this.walDataFiles){
            if (this.walDataFiles.size() == 1) {
                return this.walDataFiles.get(0);
            }
            // 根据文件名的sequence编号来获取最新的文件
            this.walDataFiles.sort(new WalDataFile.FileNameComparator());
            return this.walDataFiles.get(0);
        }
    }

    public List<WalLogData> listWalDatas() {
        return listWalDatas(new WalLogData.WalLogDataComparetor());
    }

    /**
     * 获取目录下所有的 wal data 并且进行排序
     *
     * @return
     */
    public List<WalLogData> listWalDatas(Comparator<? super WalLogData> c) {
        List<List<WalLogData>> walData = null;
        try {
            walData = Files.list(Paths.get(this.path))
                    .filter(p -> p.toFile().getName().endsWith(WalDataFile.WAL_FILE_PREFIX))
                    .map(Path::toFile)
                    .map(f -> {
                        try {
                            return WalDataFile.getWalLogDataFromFile(f);
                        } catch (IOException e) {
                            e.printStackTrace();
                            log.error(e.getMessage(), e);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return Collections.emptyList();
        }
        List<WalLogData> ds = new ArrayList<>();
        for (List<WalLogData> d : walData) {
            ds.addAll(d);
        }
        ds.sort(c);
        return ds;
    }
}
