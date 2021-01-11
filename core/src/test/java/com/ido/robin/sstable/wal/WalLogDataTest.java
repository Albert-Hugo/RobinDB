package com.ido.robin.sstable.wal;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Ido
 * @date 2021/1/6 10:37
 */
public class WalLogDataTest {


    @Test
    public void testToBytes() {
        WalLogData walLogData = new WalLogData(Cmd.PUT,"1".getBytes(),"2".getBytes(),1);
        byte[] bs = walLogData.toBytes();
        WalLogData copy  = WalLogData.fromBytes(bs);

        Assert.assertEquals(walLogData,copy);


    }
}
