package com.ido.robin.sstable;

import com.ido.robin.sstable.wal.Cmd;
import com.ido.robin.sstable.wal.WalLogData;
import com.ido.robin.sstable.wal.WalManager;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2019/8/31 16:13
 */
@Slf4j
public class SSTable implements Closeable, FileManager.SegmentFileChangeListener {
    private volatile List<SegmentFile> segmentFiles;
    private Index index;
    private String path;
    private NavigableMap<String, ExpiredData> tempBlockBeforeFlush = new ConcurrentSkipListMap<>();
    private FileManager fileManager;
    private WalManager walManager;
    /**
     * 是否每次都自动flush
     */
    private boolean flushEveryTime;

    private final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantLock.readLock();

    private static class ExpiredData {
        private String val;
        private long expiredTime;

        public ExpiredData(String val, long expiredTime) {
            this.val = val;
            this.expiredTime = expiredTime;
        }
    }

    public SSTable(String path) throws IOException {
        this(path, false);
    }

    public List<SegmentFile> getSegmentFiles() {
        return segmentFiles;
    }

    public SSTable(String path, boolean flushEveryTime) throws IOException {

        Objects.requireNonNull(path, "the data dir path can not be null");
        if (path.startsWith(".")) {
            throw new IllegalArgumentException("relative path is not supported  yet ");
        }

        if (path.endsWith(File.separator)) {
            this.path = path;
        } else {
            this.path = path + File.separator;
        }
        File pt = new File(path);
        if (!pt.exists()) {
            if (!pt.mkdir()) {
                log.error("create path {} failed ", path);
                System.exit(-1);
            }
        }


        this.flushEveryTime = flushEveryTime;

        List<File> segmentFileNames = Files.list(Paths.get(this.path)).filter(p -> p.toFile().getName().endsWith(SegmentFile.SEGMENT_FILE_SUFFIX)).map(Path::toFile).collect(Collectors.toList());
        if (segmentFileNames.isEmpty()) {
            this.segmentFiles = new CopyOnWriteArrayList<>(Collections.singletonList(new SegmentFile(this.path + UUID.randomUUID().toString().replace("-", "") + SegmentFile.SEGMENT_FILE_SUFFIX)));
        } else {

            this.segmentFiles = segmentFileNames.stream().map(fn -> {
                try {
                    return new SegmentFile(fn);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }
        walManager = new WalManager(this.path);

        recovery();

        this.index = new SparseIndexMem(segmentFiles);
        this.segmentFiles.sort(SegmentFile::compareTo);

        fileManager = new FileManager();

        if (!flushEveryTime) {
            //如果不是每次都自动flush，则开启后台线程自动flush
            fileManager.setSegmentFiles(segmentFiles);
            fileManager.setupAutoFlush(path);
        }

        fileManager.setupAutoSplitTask(path);
        fileManager.setupExpiredDataTask(path);
        fileManager.addSegmentFileChangeListener(this);
        fileManager.addSegmentFileChangeListener(this.index);

    }

    /**
     * 从write ahead log 中恢复数据
     *
     * @return 最后一个回复的entry id
     */
    long recovery() {
        List<WalLogData> ds = walManager.listWalDatas();
        if (ds == null || ds.isEmpty()) {
            return -1;
        }

        for (WalLogData d : ds) {
            Cmd cmd = d.getCmd();
            if (cmd.equals(Cmd.PUT)) {
                this.putWithoutWriteAheadLog(new String(d.getKey()), new String(d.getVal()));
            } else if (cmd.equals(Cmd.DELETE)) {
                this.removeWithoutWriteAheadLog(new String(d.getKey()));
            } else {
                log.warn("unknown cmd {}", cmd);
            }
        }
        long lastEntryId = ds.get(ds.size() - 1).getSequence();
        walManager.markRecoverySavePoint(lastEntryId);
        // discard useless log file
        walManager.cleanAfterRecovery();
        return lastEntryId;

    }


    /**
     * 批量保存
     *
     * @param keyValues
     */
    public void batchPut(List<KeyValue> keyValues) {
        try {
            FileManager.monitorWriteLock.lock();
            writeLock.lock();
            keyValues.forEach(kv -> {
                determineSegmentFileByKey(this.segmentFiles, kv.getKey()).put(kv.getKey(), kv.getVal().getBytes(), kv.getExpiredTime());
            });
            if (flushEveryTime) {
                flush();
            } else {
                keyValues.forEach(kv -> {
                    tempBlockBeforeFlush.put(kv.getKey(), new ExpiredData(kv.getVal(), kv.getExpiredTime()));
                });
            }
        } finally {
            FileManager.monitorWriteLock.unlock();
            writeLock.unlock();
        }

    }


    /**
     * 将内容按照key 在内容中排序之后，保存到file 中
     *
     * @param key
     * @param value
     */
    public void putWithoutWriteAheadLog(String key, String value) {
        batchPut(Collections.singletonList(new KeyValue(key, value)));
    }

    public void put(String key, String value) {
        walManager.append(Cmd.PUT, key, value);
        putWithoutWriteAheadLog(key, value);
    }

    /**
     * @param key
     * @param value
     * @param expiredTime 过期时间
     */
    public void put(String key, String value, long expiredTime) {
        batchPut(Collections.singletonList(new KeyValue(key, value, expiredTime)));
    }

    /**
     * decide key is going to be stored in which file
     *
     * @param segmentFiles source files
     * @param key          the key
     * @return the target file
     */
    private SegmentFile determineSegmentFileByKey(List<SegmentFile> segmentFiles, String key) {
        if (segmentFiles.size() == 1) {
            log.trace("key {} is stored in {} , key range {}, {} ", key, segmentFiles.get(0).getOriginalFileName(), segmentFiles.get(0).getHeader().keyStart, segmentFiles.get(0).getHeader().keyEnd);
            return segmentFiles.get(0);
        }
        // 决定key 放到哪一个文件中，注意segmentFiles需要先排序
        for (int i = 0; i < segmentFiles.size(); i++) {
            SegmentFile f = segmentFiles.get(i);
            if (f.keyIsBetweenIn(key)) {
                log.trace("key {} is stored in {}, key range {}, {}", key, f.getOriginalFileName(), f.getHeader().keyStart, f.getHeader().keyEnd);
                return f;
            } else {
                if (i <= segmentFiles.size() - 2) {
                    SegmentFile nextFile = segmentFiles.get(i + 1);
                    //如果key 在两个文件之中，则选择文件小的放入

                    if (nextFile.compareToKey(key) >= 0 && f.compareToKey(key) <= 0) {
                        SegmentFile targetFile = f.getLength() < nextFile.getLength() ? f : nextFile;
                        log.trace("key {} is stored in {}", key, f.getOriginalFileName());
                        return targetFile;
                    }
                }
            }
        }

        log.debug("not found any file matched key {} ", key);
        if (key.compareTo(segmentFiles.get(0).getHeader().keyStart) < 0) {
            return segmentFiles.get(0);
        }
        return segmentFiles.get(segmentFiles.size() - 1);
    }

    /**
     * 使指定key 马上过期
     *
     * @param key
     */
    public void expire(String key) {
        String val = this.get(key);
        this.put(key, val, -2);
    }

    /**
     * 设置指定key 过期时间
     *
     * @param key
     */
    public void expire(String key, long expireTime) {
        String val = this.get(key);
        this.put(key, val, expireTime);
    }

    /**
     * 获取文件中的内容呢，映射到memtable中之后，直接获取
     *
     * @param k
     * @return
     */
    public String get(String k) {
        byte[] bs = getBytes(k);
        return bs == null ? null : new String(bs);
    }

    public byte[] getBytes(String k) {
        try {
            FileManager.monitorReadLock.lock();
            readLock.lock();
            ExpiredData v = tempBlockBeforeFlush.get(k);

            if (v != null) {
                if (v.expiredTime == Block.PERMANENT) {
                    return v.val.getBytes();
                }
                if (v.expiredTime < System.currentTimeMillis()) {
                    return null;
                }

                return v.val.getBytes();

            }

            Block b = this.index.search(k);
            if (b != null) {
                log.debug("key {}  in file {} ", k, b.getFileName());
                return b.getVal();
            }
        } finally {
            FileManager.monitorReadLock.unlock();
            readLock.unlock();
        }


        return null;
    }

    /**
     * 删除值
     *
     * @param k
     * @return
     */
    public boolean remove(String k) {
        walManager.append(Cmd.DELETE, k, null);
        return removeWithoutWriteAheadLog(k);
    }

    private boolean removeWithoutWriteAheadLog(String k) {
        return batchRemove(Collections.singletonList(k));
    }


    public boolean batchRemove(List<String> ks) {

        try {
            FileManager.monitorWriteLock.lock();
            this.writeLock.lock();
            ks.forEach(k -> {

                determineSegmentFileByKey(this.segmentFiles, k).remove(k);
            });
            if (flushEveryTime) {
                flush();
            } else {
                ks.forEach(k -> {
                    tempBlockBeforeFlush.remove(k);
                });
            }
        } finally {
            FileManager.monitorWriteLock.unlock();
            this.writeLock.unlock();
        }
        return true;
    }

    /**
     * 关闭SSTable
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        flush();
        this.fileManager.shutdown();
        log.info("SSTable shutdown successfully.");
    }

    /**
     * 将新添加的内容flush 到 文件系统中。
     */
    public void flush() {
        try {

            FileManager.monitorWriteLock.lock();
            writeLock.lock();
            this.segmentFiles.forEach(SegmentFile::flush);
            //re-index
            this.index = new SparseIndexMem(this.segmentFiles);
            fileManager.addSegmentFileChangeListener(this.index);
            this.tempBlockBeforeFlush.clear();
        } finally {

            FileManager.monitorWriteLock.unlock();
            writeLock.unlock();
        }
    }

    @Override
    public void onChange(List<SegmentFile> newSgs, List<String> changedSgFiles) {
        // 将原来的 segmentFiles 中还没flush 到文件系统的block data 重新分配到新的 newSgs
        try {
            writeLock.lock();
            log.info("start to re-dispatch...");
            Set<Block> bs = new HashSet<>();
            this.segmentFiles.stream().forEach(sg -> bs.addAll(sg.blocksNotYetToFlush()));

            this.segmentFiles = new CopyOnWriteArrayList<>(newSgs);
            this.segmentFiles.sort(SegmentFile::compareTo);

            bs.forEach(b -> {
                determineSegmentFileByKey(this.segmentFiles, b.getKey()).put(b.getKey(), b.getVal());
            });
            this.segmentFiles.forEach(SegmentFile::flush);

            //重新index 文件内容
            this.index = new SparseIndexMem(segmentFiles);
            this.fileManager.setSegmentFiles(segmentFiles);
        } finally {
            log.info("re-dispatch complete...");
            writeLock.unlock();
        }
    }


    public static class State {
        List<Meta> metas;
        private String path;
        private long dataSize;
        private int fileCount;

        public List<Meta> getMetas() {
            return metas;
        }

        public String getPath() {
            return path;
        }

        public long getDataSize() {
            return dataSize;
        }

        public int getFileCount() {
            return fileCount;
        }

        @Override
        public String toString() {
            return "State{" +
                    "metas=" + metas +
                    ", path='" + path + '\'' +
                    ", dataSize=" + dataSize +
                    ", fileCount=" + fileCount +
                    '}';
        }
    }

    private static class Meta {
        SegmentHeader metadata;
        String filename;

        public Meta(SegmentHeader metadata, String filename) {
            this.metadata = metadata;
            this.filename = filename;
        }

        @Override
        public String toString() {
            return "Meta{" +
                    "metadata=" + metadata +
                    ", filename='" + filename + '\'' +
                    '}';
        }
    }

    /**
     * 获取sstable 的元信息
     *
     * @return
     */
    public State getState() {
        List<Meta> metas = this.segmentFiles.stream().map(s -> {

            return new Meta(s.getHeader(), s.getOriginalFileName());

        }).collect(Collectors.toList());

        State state = new State();
        state.metas = metas;
        state.fileCount = metas.size();
        state.path = this.path;
        state.dataSize = metas.stream().map(s -> s.metadata.fileLen).reduce(0L, (a, b) -> a + b);
        return state;

    }
}
