package com.ido.robin.sstable.wal;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * @author Ido
 * @date 2021/1/6 13:30
 */
public class WalDataFileTest {


    @Test
    public void testAppend() throws IOException {
        WalDataFile file = new WalDataFile("test.wal");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            file.append(new WalLogData(Cmd.PUT, "k".getBytes(), "d".getBytes(), 1));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }

    @Test
    public void testGetWalLogDataFromFile() throws IOException {
        List<WalLogData> result = WalDataFile.getWalLogDataFromFile("test.wal");
        System.out.println(result.size());

    }
}
