package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Ido
 * @date 2019/1/30 15:00
 */
@Slf4j
public class EmptyValueBlockReader implements BlockReader {
    public final static EmptyValueBlockReader EMPTY_VALUE_BLOCK_READER = new EmptyValueBlockReader();

    private EmptyValueBlockReader() {
    }

    @Override
    public Block getBlock(ByteBuffer fileBf, int keySize, int valSize, int offset, long expiredTime,byte isCompress) {
        byte[] key = new byte[keySize];//获取到key 的 offset
        fileBf.get(key);

        String javaVersion = System.getProperty("java.version");
        if (javaVersion.length() >= 3 && Double.valueOf(javaVersion.substring(0, 3)) < 1.9) {
            //兼容java 8 以下JDK 版本的兼容
            byte[] val = new byte[valSize];//获取到val 的 offset
            fileBf.get(val);
        } else {
            fileBf.position(fileBf.position() + valSize);//java 8 不支持这种写法
        }
        Block b = new Block();
        b.expiredTime = expiredTime;
        b.isCompress = isCompress;
        b.keyLen = keySize;
        b.valLen = valSize;
        b.key = new String(key);
        b.offset = offset;
        return b;
    }

    @Override
    public Block getBlock(DataInputStream is, int keySize, int valSize, int offset, long expiredTime,byte isCompress) {
        byte[] key = new byte[keySize];//获取到key 的 offset
        try {
            is.read(key);
            is.skip(valSize);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        Block b = new Block();
        b.expiredTime = expiredTime;
        b.isCompress = isCompress;
        b.keyLen = keySize;
        b.valLen = valSize;
        b.key = new String(key);
        b.offset = offset;
        return b;

    }
}
