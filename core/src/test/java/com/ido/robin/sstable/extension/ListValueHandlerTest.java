package com.ido.robin.sstable.extension;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Ido
 * @date 2019/3/10 13:28
 */
public class ListValueHandlerTest {

    private static String path = "D:\\robin-data\\";

    @Test
    public void testAdd() throws IOException {
        SSTablePlus ssTable = new SSTablePlus(path);

        ssTable.listAdd("ido", "1".getBytes());
        ssTable.listAdd("ido", "2".getBytes());
        ssTable.flush();
        List result = ssTable.getList("ido");
        Map<String, List<String>> allList = ssTable.getAllList(String.class);

        System.out.println(allList);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("1", new String((byte[]) result.get(0)));
        Assert.assertEquals("2", new String((byte[]) result.get(1)));

    }
}
