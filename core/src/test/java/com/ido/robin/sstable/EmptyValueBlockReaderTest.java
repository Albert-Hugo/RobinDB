package com.ido.robin.sstable;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Ido
 * @date 2020/12/22 18:32
 */
public class EmptyValueBlockReaderTest {

    @Test
    public void test(){
        Block b = new Block("1","12".getBytes());
        ByteBuffer byteBuffer =  ByteBuffer.wrap(b.bytes());
        byteBuffer.getLong();
        byteBuffer.getLong();
        byteBuffer.getLong();
        Block read = EmptyValueBlockReader.EMPTY_VALUE_BLOCK_READER.getBlock(byteBuffer,"1".getBytes().length,"12".getBytes().length,0,-1);

        Assert.assertEquals("1",read.key);
        Assert.assertNull(null);


    }


    @Test
    public void testWholeValue(){
        Block b = new Block("1","12".getBytes());
        ByteBuffer byteBuffer =  ByteBuffer.wrap(b.bytes());
        byteBuffer.getLong();
        byteBuffer.getLong();
        byteBuffer.getLong();
        Block read = WholeBlockReader.WHOLE_BLOCK_READER.getBlock(byteBuffer,"1".getBytes().length,"12".getBytes().length,0,-1);

        Assert.assertEquals("1",read.key);
        Assert.assertEquals("12",new String(read.val));


    }
}
