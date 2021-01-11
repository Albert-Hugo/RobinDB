package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Ido
 * @date 2019/8/31 16:31
 */
@Slf4j
public class Block implements Comparable<Block> {
    protected static int KEY_LEN_SIZE = 8;
    protected static int VAL_LEN_SIZE = 8;
    protected static int EXPIRED_TIME_SIZE = 8;
    public final static long PERMANENT = -1;
    String key;
    long keyLen;
    /**
     * 过期时间
     */
    long expiredTime = PERMANENT;
    long valLen;
    byte[] val;
    /**
     * 所属文件 不保存到文件系统中
     */
    private String fileName;
    /**
     * 内容在文件中的offset
     */
    int offset;

    public int getOffset() {
        return offset;
    }

    Block setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    String getFileName() {
        return fileName;
    }

    public Block(String key, byte[] val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);
        this.key = key;
        this.val = val;
        this.keyLen = key.getBytes().length;
        this.valLen = val.length;

    }

    /**
     * @param key
     * @param val
     * @param expiredTime millis second
     */
    public Block(String key, byte[] val, long expiredTime) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);
        this.key = key;
        this.val = val;
        this.keyLen = key.getBytes().length;
        this.valLen = val.length;
        if (expiredTime != PERMANENT) {
            this.expiredTime = System.currentTimeMillis() + expiredTime;
        }

    }

    public boolean isExpired() {
        if(expiredTime == PERMANENT){
            return false;
        }
        return expiredTime < System.currentTimeMillis();
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }


    public int getBlockLength() {
        int velSize = this.getVal().length;
        int keySize = this.getKey().getBytes().length;
        return velSize + keySize + KEY_LEN_SIZE + VAL_LEN_SIZE + EXPIRED_TIME_SIZE;

    }

    public byte[] getVal() {
        return val;
    }

    Block() {
    }

    public Block setVal(byte[] val) {
        this.val = val;
        return this;
    }


    public static List<Block> read(byte[] blockData) {
        return read(0, blockData, "", WholeBlockReader.WHOLE_BLOCK_READER);
    }

    /**
     * 将 byte[] 转换成 block List
     *
     * @param blockData block data
     * @param fileName  the file name belong to the block
     * @return
     */
    public static List<Block> read(int headerLen, byte[] blockData, String fileName, BlockReader reader) {
        if (blockData == null || blockData.length == 0) {
            return new ArrayList<>();
        }
        ByteBuffer fileBf = ByteBuffer.wrap(blockData);
        List<Block> blocks = new ArrayList<>();
        while (true) {
            int offset = headerLen + fileBf.position();
            long expiredTime = fileBf.getLong();
            int keySize = (int) fileBf.getLong();
            int valSize = (int) fileBf.getLong();
            if (keySize == 0 && valSize == 0) break;

            Block b = reader.getBlock(fileBf, keySize, valSize, offset, expiredTime);
            b.fileName = fileName;
            blocks.add(b);
            if (fileBf.remaining() < (KEY_LEN_SIZE + VAL_LEN_SIZE + EXPIRED_TIME_SIZE)) {
                break;
            }
        }


        return blocks;

    }

    /**
     * 将一个block变成 bytes
     *
     * @return
     */
    public byte[] bytes() {
        int velSize = this.val.length;
        int keySize = this.key.getBytes().length;
        ByteBuffer bf = ByteBuffer.allocate(KEY_LEN_SIZE + VAL_LEN_SIZE + EXPIRED_TIME_SIZE + velSize + keySize);
        bf.putLong(this.expiredTime);
        bf.putLong(this.keyLen);
        bf.putLong(this.valLen);
        bf.put(this.key.getBytes());
        bf.put(this.val);
        return bf.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Block block = (Block) o;
        return key.equals(block.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }


    @Override
    public int compareTo(Block o) {
        return this.getKey().compareTo(o.getKey());
    }

    @Override
    public String toString() {
        return "Block{" +
                "key='" + key + '\'' +
                ", keyLen=" + keyLen +
                ", valLen=" + valLen +
                ", val='" + val + '\'' +
                ", fileName='" + fileName + '\'' +
                ", offset=" + offset +
                '}';
    }
}
