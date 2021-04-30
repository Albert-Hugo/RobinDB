package com.ido.robin.sstable;

import org.junit.Test;

import java.io.IOException;

/**
 * @author Ido
 * @date 2019/1/11 16:04
 */
public class FileManagerTest {
    private static String path = "D:\\robin-data";

    public static void main(String[] args) {
        new FileManager().setupAutoSplitTask(path);
    }



    @Test
    public void testAutoDeleteExpiredData() throws IOException {
        SSTable ssTable = new SSTable(path);

        ssTable.put("fds","12",System.currentTimeMillis() + 1000);
        ssTable.flush();
        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }





    }
}
