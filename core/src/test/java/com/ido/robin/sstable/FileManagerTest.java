package com.ido.robin.sstable;

import junit.framework.TestListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * @author Ido
 * @date 2019/1/11 16:04
 */
public class FileManagerTest {
    private static String path = "D:\\robin-data";


    @Before
    public void setup() throws IOException {
        SegmentFile s = new SegmentFile(path + "\\" + "test.seg");

        for (int i = 0; i < 100; i++) {
            s.put(UUID.randomUUID().toString(), UUID.randomUUID().toString().getBytes());
        }
        s.close();
    }

    static class TestListener implements FileManager.SegmentFileChangeListener {

        @Override
        public void onChange(List<SegmentFile> segmentFiles, List<String> changedSgFiles) {
            System.out.println(this + " notify !");
        }
    }

    @Test
    public void testAutoSplitFileListenerWillRemoveWhenUsingWeakReferrence() throws IOException, InterruptedException {
        FileManager fileManager = new FileManager(new LocalFileSystemSplitor(10));
        fileManager.setSplitTimeInterval(5);
        fileManager.setupAutoSplitTask(path);
        TestListener l1 = new TestListener();
        fileManager.addSegmentFileChangeListener(l1);
        Thread.sleep(10000);
        TestListener l2 = new TestListener();
        l1 = null;
        SegmentFile s = new SegmentFile(path + "\\" + "test.seg");

        for (int i = 0; i < 100; i++) {
            s.put(UUID.randomUUID().toString(), UUID.randomUUID().toString().getBytes());
        }
        s.close();
        System.gc();
        fileManager.addSegmentFileChangeListener(l2);
        Thread.sleep(5000);
        Assert.assertEquals(1, fileManager.getListeners().size());
        Thread.sleep(10000);


    }

    @Test
    public void testAutoDeleteExpiredData() throws IOException {
        SSTable ssTable = new SSTable(path);

        ssTable.put("fds", "12", System.currentTimeMillis() + 1000);
        ssTable.flush();
        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
