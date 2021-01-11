package com.ido.robin.sstable.extension;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @author Ido
 * @date 2019/3/10 13:28
 */
public class MapValuePluginTest {
    private static String path = "D:\\robin-data\\";

    @Test
    public void testAdd() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.mapAdd("ido", "name", "ido");
        ssTable.mapAdd("ido", "age", "18");
        ssTable.flush();
        Map result = ssTable.getMap("ido");
        System.out.println(result);
        Assert.assertEquals(2, result.size());

    }
}
