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

        ssTable.setAdd("ido", "1");
        ssTable.setAdd("ido", "2");
        ssTable.flush();
        Set result = ssTable.getSet("ido");
        System.out.println(result);
        Assert.assertEquals(2, result.size());

    }

    @Test
    public void testClear() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.setAdd("ido", "1");
        ssTable.setAdd("ido", "2");
        ssTable.flush();
        ssTable.setClear("ido");
        ssTable.flush();
        Assert.assertTrue(ssTable.getSet("ido").isEmpty());

    }

    @Test
    public void testRemove() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.setAdd("ido", "1");
        ssTable.setAdd("ido", "2");
        ssTable.flush();
        boolean result = ssTable.setRemove("ido", "1");
        ssTable.flush();
        Assert.assertTrue(result);
        Assert.assertFalse(ssTable.getSet("ido").contains("1"));

    }
}
