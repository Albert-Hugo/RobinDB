package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reader to read a whole block content from target bytes
 *
 * @author Ido
 * 2019/1/30 15:00
 */
@Slf4j
public class WholeBlockReader implements BlockReader {
    public final static WholeBlockReader WHOLE_BLOCK_READER = new WholeBlockReader();

    private WholeBlockReader() {
    }

    @Override
    public Block getBlock(ByteBuffer fileBf, int keySize, int valSize, int offset, long expiredTime) {
        byte[] key = new byte[keySize];//获取到key 的 offset
        byte[] val = new byte[valSize];//获取到val 的 offset
        fileBf.get(key);
        fileBf.get(val);

        Block b = new Block();
        b.expiredTime = expiredTime;
        b.keyLen = keySize;
        b.valLen = valSize;
        b.key = new String(key);
        b.val = val;
        b.offset = offset;
        return b;
    }

    @Override
    public Block getBlock(DataInputStream is, int keySize, int valSize, int offset, long expiredTime) {
        byte[] key = new byte[keySize];//获取到key 的 offset
        byte[] val = new byte[valSize];//获取到val 的 offset
        try {
            is.read(key);
            is.read(val);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        Block b = new Block();
        b.expiredTime = expiredTime;
        b.keyLen = keySize;
        b.valLen = valSize;
        b.key = new String(key);
        b.val = val;
        b.offset = offset;
        return b;

    }
}
