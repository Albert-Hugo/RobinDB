package com.ido.robin.sstable.extension;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

/**
 * @author Ido
 * @date 2019/3/10 13:28
 */
public class SetValuePluginTest {
    private static String path = "D:\\robin-data\\";

    @Test
    public void testAdd() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.setAdd("set", "1");
        ssTable.setAdd("set", "2");
        ssTable.flush();
        Set result = ssTable.getSet("set");
        System.out.println(result);
        Assert.assertEquals(2, result.size());
        ssTable.close();

    }

    @Test
    public void testClear() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.setAdd("set", "1");
        ssTable.setAdd("set", "2");
        ssTable.flush();
        ssTable.setClear("set");
        ssTable.flush();
        Assert.assertTrue(ssTable.getSet("set").isEmpty());
        ssTable.close();

    }

    @Test
    public void testRemove() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.setAdd("set", "1");
        ssTable.setAdd("set", "2");
        ssTable.flush();
        boolean result = ssTable.setRemove("set", "1");
        ssTable.flush();
        Assert.assertTrue(result);
        Assert.assertFalse(ssTable.getSet("set").contains("1"));
        ssTable.close();

    }
}
