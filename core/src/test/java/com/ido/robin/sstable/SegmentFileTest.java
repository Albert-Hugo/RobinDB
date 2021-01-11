package com.ido.robin.sstable;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author Ido
 * @date 2019/1/1 10:08
 */
public class SegmentFileTest {
    private static String path = "D:\\robin-data\\";
    private String key;
    private String removeKey;
    private String removeVel;
    private String val;
    private String filename;
    private Random random = new Random();
    private int initSize = 1000;

    @Before
    public void setup() {
        try {
            SegmentFile segmentFile = new SegmentFile(path + "test.seg");
            for (int i = 0; i < initSize; i++) {
                String k = RandomStringUtils.randomAlphanumeric(random.nextInt(5) + 20);
                String v = RandomStringUtils.randomAlphanumeric(random.nextInt(256) + 1);
                if (i == 5) {
                    key = k;
                    val = v;
                    System.out.println(k);
                    System.out.println(v);
                }

                if (i == 8) {
                    removeKey = k;
                    removeVel = v;
                    System.out.println(removeKey);
                    System.out.println(removeVel);
                }
                segmentFile.put(k, v.getBytes());
            }
            segmentFile.flush();
            filename = segmentFile.getOriginalFileName();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testFindByPattern() throws IOException {
        SegmentFile segmentFile = new SegmentFile(filename);
        segmentFile.put("test1", "dsada".getBytes());
        segmentFile.put("test2", "dsada".getBytes());
        segmentFile.put("test3", "dsada".getBytes());
        segmentFile.put("test4", "dsada".getBytes());
        segmentFile.put("test321423", "dsada".getBytes());
        segmentFile.put("testfagasg", "dsada".getBytes());
        List list = segmentFile.find("^test\\w*");
        Assert.assertEquals(6, list.size());
        System.out.println(list.toString());

    }




    @Test
    public void testRemove() throws IOException {
        SegmentFile segmentFile = new SegmentFile(filename);
        segmentFile.remove(removeKey);
        segmentFile.flush();
        Assert.assertNull(segmentFile.get(removeKey));
    }

    @Test
    public void testExpired() throws IOException {
        SegmentFile segmentFile = new SegmentFile(filename);
        segmentFile.put("expired", "1".getBytes(), -1000);
        segmentFile.flush();
        Assert.assertNull(segmentFile.get("expired"));
    }


    @Test
    public void print() throws IOException {
        //yESalXY8jgOyv0EagpEEX  wLQOZ7NeGmf5
        SegmentFile segmentFile = new SegmentFile(path + "\\10.seg");
        for (Block b : segmentFile.getBlockList()) {
//            if(b.getKey().equals("5iueKXd"))
            System.out.println(b.getKey());
        }
    }
}
