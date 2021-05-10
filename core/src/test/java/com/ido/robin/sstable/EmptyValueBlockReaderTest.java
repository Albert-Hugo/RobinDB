package com.ido.robin.sstable;

import com.ido.robin.common.CompressUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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
        byteBuffer.get();
        byteBuffer.getLong();
        byteBuffer.getLong();
        byte isComress = 0;
        Block read = EmptyValueBlockReader.EMPTY_VALUE_BLOCK_READER.getBlock(byteBuffer,"1".getBytes().length,"12".getBytes().length,0,-1,isComress);

        Assert.assertEquals("1",read.key);
        Assert.assertNull(null);


    }


    @Test
    public void testWholeValue(){
        Block b = new Block("1","12".getBytes());
        ByteBuffer byteBuffer =  ByteBuffer.wrap(b.bytes());
        byteBuffer.getLong();
        byteBuffer.get();
        byteBuffer.getLong();
        byteBuffer.getLong();
        byte isComress = 0;
        Block read = WholeBlockReader.WHOLE_BLOCK_READER.getBlock(byteBuffer,"1".getBytes().length,"12".getBytes().length,0,-1,isComress);

        Assert.assertEquals("1",read.key);
        Assert.assertEquals("12",new String(read.val));


    }


    @Test
    public void testWholeValueWithCompress() throws IOException {
        String val = "1DAFADDSADADADAF2";
        Block b = new Block("1",val.getBytes());
        byte[] comprssData = CompressUtil.compress(val.getBytes());
        ByteBuffer byteBuffer =  ByteBuffer.wrap(b.bytes());
        byteBuffer.getLong();
        byteBuffer.get();
        byteBuffer.getLong();
        byteBuffer.getLong();
        byte isComress = 1;
        Block read = WholeBlockReader.WHOLE_BLOCK_READER.getBlock(byteBuffer,"1".getBytes().length,comprssData.length,0,-1,isComress);

        Assert.assertEquals("1",read.key);
        Assert.assertEquals("1DAFADDSADADADAF2",new String(read.val));


    }
}
