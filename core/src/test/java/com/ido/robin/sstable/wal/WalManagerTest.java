package com.ido.robin.sstable.wal;

import org.junit.Test;

/**
 * @author Ido
 * @date 2021/1/6 17:25
 */
public class WalManagerTest {

    @Test
    public void testContruct(){
        WalManager walManager = new WalManager("D:\\robin-data\\");
        walManager.append(Cmd.DELETE,"fs","d");

    }
}
