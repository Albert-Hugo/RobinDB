package com.ido.robin.sstable;

import java.io.DataInputStream;
import java.nio.ByteBuffer;

/**
 * @author Ido
 * @date 2019/1/30 14:59
 */
public interface BlockReader {
    /**
     * construct a block by reading data from bytes buffer
     *
     * @param fileBf  the bytes buffer
     * @param keySize the key size of block
     * @param valSize the value size of block
     * @param offset  the block's offset in the original file bytes content
     * @return
     */
    Block getBlock(ByteBuffer fileBf, int keySize, int valSize, int offset, long expiredTime,byte isCompress);

    /**
     * construct a block by reading data from input stream
     *
     * @param fileBf  the bytes buffer
     * @param keySize the key size of block
     * @param valSize the value size of block
     * @param offset  the block's offset in the original file bytes content
     * @return
     */
    Block getBlock(DataInputStream fileBf, int keySize, int valSize, int offset, long expiredTime,byte isCompress);
}
