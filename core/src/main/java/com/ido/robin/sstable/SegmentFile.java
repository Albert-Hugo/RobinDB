package com.ido.robin.sstable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ido.robin.common.FileUtil;
import com.ido.robin.sstable.dto.Meta;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * 保存数据内容的文件
 *
 * @author Ido
 * @date 2019/8/31 16:15
 */
@Slf4j
public class SegmentFile implements Closeable, SortableFileName {
    /**
     * 文件大小,字节
     */
    private long length;
    /**
     * block list 的长度
     */
    private long blockLen;
    /**
     * 文件头部大小
     */
    private long headerLen;
    final static String SEGMENT_FILE_SUFFIX = ".seg";
    /**
     * 文件头
     */
    private SegmentHeader header;
    /**
     * flush 之前的文件名
     */
    private String originalFileName;
    //    private HeaderAndBlockData cacheFileContent;
    private CacheLoader<Boolean, HeaderAndBlockData> cacheLoader = new CacheLoader<Boolean, HeaderAndBlockData>() {
        @Override
        public HeaderAndBlockData load(Boolean s) {
            return loadDataFromFS();
        }
    };

    LoadingCache<Boolean, HeaderAndBlockData> cacheFileContent = CacheBuilder.newBuilder()
            .expireAfterAccess(120, TimeUnit.SECONDS)
            .build(cacheLoader);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * 待添加的新数据
     */
    private ConcurrentSkipListSet<Block> toAddBlockList = new ConcurrentSkipListSet<>();
    private Queue<BlockCommand> cmds = new PriorityBlockingQueue<>();
    /**
     * 待删除的数据
     */
    private ConcurrentSkipListSet<Block> toRemoveBlockList = new ConcurrentSkipListSet<>();

    private String path;

    private static class BlockCommand implements Comparable<BlockCommand> {
        final static int REMOVE = 0;
        final static int ADD = 1;
        int cmd;
        long opTime;
        Block b;


        @Override
        public int compareTo(BlockCommand o) {
            return (int) (this.opTime - o.opTime);
        }

        @Override
        public String toString() {
            return "BlockCommand{" +
                    "cmd=" + cmd +
                    ", opTime=" + opTime +
                    ", b=" + b +
                    '}';
        }

        public static void main(String[] args) {
            Queue<BlockCommand> testcmds = new PriorityBlockingQueue<>();
            BlockCommand cmd = new BlockCommand();
            cmd.b = null;
            cmd.opTime = System.currentTimeMillis() + 100;
            cmd.cmd = BlockCommand.REMOVE;
            BlockCommand cmd1 = new BlockCommand();
            cmd1.b = null;
            cmd1.opTime = System.currentTimeMillis() + 2;
            cmd1.cmd = BlockCommand.REMOVE;
            testcmds.add(cmd);
            testcmds.add(cmd1);

            System.out.println(testcmds.toString());
        }
    }

    BlockCommand buildRemoveCmd(Block b) {
        BlockCommand cmd = new BlockCommand();
        cmd.b = b;
        cmd.opTime = System.currentTimeMillis();
        cmd.cmd = BlockCommand.REMOVE;
        return cmd;
    }

    BlockCommand buildAddCmd(Block b) {
        BlockCommand cmd = new BlockCommand();
        cmd.b = b;
        cmd.opTime = System.currentTimeMillis();
        cmd.cmd = BlockCommand.ADD;
        return cmd;
    }


    SegmentFile(String name) throws IOException {
        File f = new File(name);
        this.init(f);
    }

    public ConcurrentSkipListSet<Block> getToAddBlockList() {
        return toAddBlockList;
    }

    public ConcurrentSkipListSet<Block> getToRemoveBlockList() {
        return toRemoveBlockList;
    }

    /**
     * 清空队列中 待添加中的所有block
     *
     * @return
     */
    private void consumeCmdsToBlocks() {
        BlockCommand cmd;
        while ((cmd = this.cmds.poll()) != null) {
            if (cmd.cmd == BlockCommand.REMOVE) {
                if (toAddBlockList.contains(cmd.b)) {
                    toAddBlockList.remove(cmd.b);
                } else {
                    toRemoveBlockList.add(cmd.b);
                }
            } else {
                if (toRemoveBlockList.contains(cmd.b)) {
                    toRemoveBlockList.remove(cmd.b);
                } else {
                    if (!toAddBlockList.add(cmd.b)) {//如果没有添加成功，先把原来的删除掉
                        toAddBlockList.remove(cmd.b);
                        toAddBlockList.add(cmd.b);
                    }
                }
            }
        }

    }

    public Meta getMetaData() {
        consumeCmdsToBlocks();
        Meta meta = new Meta();
        TreeSet<Block> fileBlockData = getLatestBlocks();
        byte[] blockDatas = blockListToBytes(fileBlockData);
        meta.setFilename(this.originalFileName);
        meta.setBlockListSize(fileBlockData.size());
        meta.setFileLen(blockDatas.length);
        meta.setKeyEnd(this.header.keyEnd);
        meta.setKeyStart(this.header.keyStart);
        return meta;
    }

    public SegmentHeader getHeader() {
        return this.header;
    }

    /**
     * 指定的key 应该存放在这个segment file 中
     *
     * @param key
     * @return true 应该在这个文件中
     */
    boolean keyIsBetweenIn(String key) {
        return key.compareTo(header.keyStart) >= 0 && key.compareTo(header.keyEnd) <= 0;
    }

    void put(String k, byte[] v) {
        put(k, v, Block.PERMANENT);
    }

    void put(String k, byte[] v, long expiredTime) {
        if (k == null || k.isEmpty()) {
            throw new IllegalArgumentException("key can not be empty");
        }
        Block b = new Block(k, v, expiredTime);
        cmds.add(buildAddCmd(b));
    }

    /**
     * 删除k 指定的内容
     *
     * @param k
     * @return
     */
    void remove(String k) {
        Block b = new Block(k, "".getBytes());
        cmds.add(buildRemoveCmd(b));

    }

    public int getCmdsSize() {
        return cmds.size();
    }

    /**
     * 获取所有还没flush的blocks，并清空队列
     *
     * @return
     */
    Set<Block> blocksNotYetToFlush() {
        consumeCmdsToBlocks();
        Set<Block> bs = new HashSet<>();
        bs.addAll(toAddBlockList);
        bs.removeAll(toRemoveBlockList);
        this.toAddBlockList.clear();
        this.toRemoveBlockList.clear();
        log.debug("data not flush in memory size : [{}]", bs.size());
        return bs;
    }


    void addBlockList(List<Block> blocks) {
        this.toAddBlockList.addAll(blocks);
    }

    public long getLength() {
        return length;
    }

    public long getBlockLen() {
        return blockLen;
    }

    public long getHeaderLen() {
        return headerLen;
    }

    /**
     * find block by key
     *
     * @param k the key
     * @return
     */
    Block get(String k) {
        consumeCmdsToBlocks();
        Block b = new Block();
        b.setKey(k);
        if (toRemoveBlockList.contains(b)) {
            return null;
        }
        b = binarySearch(k, getBlockList());
        if (b == null) {
            return null;
        }
        //如果已经过期，则返回null
        if (b.expiredTime != Block.PERMANENT && b.expiredTime < System.currentTimeMillis()) {
            return null;
        }
        return b;

    }


    /**
     * find data matching key pattern
     *
     * @param keyPattern the regex pattern
     * @return the result list
     */
    List<Block> find(String keyPattern) {
        List<Block> blocks = getBlockList();
        Pattern pattern = Pattern.compile(keyPattern);
        List<Block> result = new ArrayList<>();
        for (Block b : blocks) {
            if (pattern.matcher(b.getKey()).find()) {
                result.add(b);
            }
        }
        return result;

    }

    private static Block binarySearch(String key, List<Block> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return null;
        }
        Block middle = blocks.get(blocks.size() / 2);
        int c = middle.getKey().compareTo(key);
        while (c != 0) {

            if (c < 0) {
                blocks = blocks.subList(blocks.size() / 2, blocks.size());
                middle = blocks.get(blocks.size() / 2);
                c = middle.getKey().compareTo(key);
            } else {
                blocks = blocks.subList(0, blocks.size() / 2);
                middle = blocks.get(blocks.size() / 2);
                c = middle.getKey().compareTo(key);
            }

            if (blocks.size() == 1 && c != 0) {
                return null;
            }
        }
        return middle;
    }

    @Override
    public void close() throws IOException {
        flush();
    }


    @Override
    public int compareToKey(String key) {
        return this.header.keyStart.compareTo(key);
    }

    @Override
    public String compareKey() {
        return this.header.keyStart;
    }

    @Override
    public int compareTo(SortableFileName o) {
        return this.compareKey().compareTo(o.compareKey());
    }


    SegmentFile(File f) throws IOException {
        this.init(f);
    }

    public String getPath() {
        return path;
    }

    private void init(File f) throws IOException {
        if (!f.exists()) {
            if (!f.createNewFile()) {
                log.error("file {} create failed ", originalFileName);
            }
        }
        this.path = f.getParent() + File.separator;
        this.originalFileName = this.path + f.getName();
        header = SegmentHeader.getHeaderInfo(this.originalFileName);
        this.headerLen = header.headerLength;
    }


    public static byte[] blockListToBytes(NavigableSet<Block> fileBlockData) {
        int totalBlockSize = calcBlockSize(fileBlockData);
        ByteBuffer bf = ByteBuffer.allocate(totalBlockSize);
        for (Block b : fileBlockData) {
            bf.put(b.bytes());
        }

        return bf.array();
    }

    private static int calcBlockSize(Set<Block> blockList) {
        int total = 0;
        for (Block b : blockList) {
            total += b.getBlockLength();
        }
        return total;
    }


    /**
     * 获取block List ，包括内存中的数据
     *
     * @return
     */
    public List<Block> getBlockList(BlockReader blockReader) {
        try {
            this.lock.readLock().lock();
            List<Block> dataInFs = Block.read((int) this.headerLen, readBlockData(), this.originalFileName, blockReader);
            consumeCmdsToBlocks();
            dataInFs.addAll(toAddBlockList);
            dataInFs.removeAll(toRemoveBlockList);
            return dataInFs;
        } catch (Throwable ex) {
            throw ex;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 获取block List ，包括内存中的数据
     *
     * @return
     */
    public List<Block> getBlockList() {
        return getBlockList(WholeBlockReader.WHOLE_BLOCK_READER);
    }

    /**
     * 获取block List ，包括内存中的数据
     *
     * @return
     */
    public List<Block> getEmptyValueBlockList() {
        return getBlockList(EmptyValueBlockReader.EMPTY_VALUE_BLOCK_READER);
    }


    private boolean fileContentNotChanged() {
        consumeCmdsToBlocks();
        if (this.toAddBlockList.isEmpty() && this.toRemoveBlockList.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("file {}  is update to date , no need to flush", this.originalFileName);
            }
            return true;
        }
        return false;
    }

    /**
     * 将 内存 的数据写到文件系统中
     */
    void flush() {
        synchronized (this.originalFileName.intern()) {

            if (fileContentNotChanged()) {
                return;
            }
            FileOutputStream fs = null;
            try {
                lock.writeLock().lock();

                TreeSet<Block> fileBlockData = getLatestBlocks();

                String oldFile = this.originalFileName;

                byte[] blockDatas = blockListToBytes(fileBlockData);
                updateHeader(fileBlockData, blockDatas.length);

                byte[] header = this.header.toBytes();

                fs = new FileOutputStream(this.originalFileName);
                fs.write(header);
                fs.write(blockDatas);
                fs.flush();

                if (!oldFile.equals(this.originalFileName)) {
                    File old = new File(oldFile);
                    if (!old.delete()) {
                        log.error("old segment file:{}  delete failed ", oldFile);
                    }
                    log.debug("old file {} is deleted ", oldFile);
                }

                this.toAddBlockList.clear();
                this.toRemoveBlockList.clear();
                this.cacheFileContent.put(true, new HeaderAndBlockData());


            } catch (IOException e) {

                log.error(e.getMessage(), e);
                return;
            } finally {
                FileUtil.closeQuietly(fs);
                lock.writeLock().unlock();
            }


            log.debug("file {} flush successfully", this.originalFileName);
        }
    }

    /**
     * 清空队列中的的数据，并从磁盘上获取源数据，再merge之后，得到最新的 block list
     *
     * @return 最新的block list
     */
    private TreeSet<Block> getLatestBlocks() {
        //先加载旧文件中的内容到blockList中
        HeaderAndBlockData headerAndBlockData = loadData();

        TreeSet<Block> fileBlockData = new TreeSet<>(Block.read((int) this.headerLen, headerAndBlockData.blocks, this.originalFileName, WholeBlockReader.WHOLE_BLOCK_READER));

        for (Block b : toAddBlockList) {
            if (!fileBlockData.add(b)) {
                fileBlockData.remove(b);
                fileBlockData.add(b);
            }
        }

        fileBlockData.removeAll(toRemoveBlockList);
        return fileBlockData;
    }

    private void updateHeader(NavigableSet<Block> fileBlockData, int blockLen) {
        if (fileBlockData.isEmpty()) {
            this.header.keyStart = "";
            this.header.keyEnd = "";
        } else {
            this.header.keyStart = fileBlockData.first().getKey();
            this.header.keyEnd = fileBlockData.last().getKey();

        }
        this.header.endLen = this.header.keyEnd.getBytes().length;
        this.header.startLen = this.header.keyStart.getBytes().length;
        this.header.headerLength = 8 + 8 + 8 + 8 + 8 + header.endLen + header.startLen;
        this.header.blockListSize = fileBlockData.size();
        this.header.fileLen = this.headerLen + blockLen;
        this.header.segmentFileName = this.originalFileName;
        this.header.segmentFileNameLen = this.originalFileName.getBytes().length;

    }


    /**
     * 获取整个文件的内容,不包括没有flush的data
     *
     * @return
     */
    private byte[] readBlockData() {

        return loadData().blocks;
    }


    private static class HeaderAndBlockData {
        private byte[] header;
        private byte[] blocks;


    }

    private HeaderAndBlockData loadDataFromFS() {
        FileInputStream file = null;
        try {
            lock.readLock().lock();
            file = new FileInputStream(this.originalFileName);
            byte[] fileData = new byte[file.available()];
            if (file.available() == 0) {
                return new HeaderAndBlockData();
            }

            this.length = file.available();
            file.read(fileData);

            ByteBuffer byteBuffer = ByteBuffer.wrap(fileData);

            int headerLen = (int) byteBuffer.getLong();

            int fileLen = fileData.length;

            byte[] hd = Arrays.copyOfRange(fileData, 0, headerLen);
            byte[] blocks = Arrays.copyOfRange(fileData, headerLen, fileLen);

            HeaderAndBlockData headBlock = new HeaderAndBlockData();
            headBlock.header = hd;

            headBlock.blocks = blocks;

            this.headerLen = headerLen;
            this.blockLen = blocks.length;
            this.header = SegmentHeader.fromBytes(hd);
            return headBlock;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            FileUtil.closeQuietly(file);
            lock.readLock().unlock();
        }
        return null;
    }

    /**
     * 将文件中的内容映射出来,不包括还在内容中没有flush的数据内容
     *
     * @return
     */
    private HeaderAndBlockData loadData() {
        HeaderAndBlockData cache = null;
        try {
            cache = cacheFileContent.get(true);
        } catch (ExecutionException e) {
            log.info(e.getMessage(), e);
            return loadDataFromFS();
        }
        if (fileContentNotChanged() && cache != null && cache.blocks != null) {
            return cache;
        }
        return loadDataFromFS();


    }

    /**
     * 获取旧文件名
     *
     * @return
     */
    String getOriginalFileName() {
        return this.originalFileName;
    }
}
