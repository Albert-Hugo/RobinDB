package com.ido.robin.sstable;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * @author Ido
 * @date 2019/1/25 10:46
 */
public class SegmentHeaderTest {
    private static String path = "D:\\生产已经存在的服务评价\\";
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
            SegmentFile segmentFile = new SegmentFile(path+ UUID.randomUUID().toString().replace("-",""));
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
    public void testHeaderInfo() throws IOException {
        SegmentHeader header = SegmentHeader.getHeaderInfo(filename);
        SegmentFile segmentFile = new SegmentFile(filename);
        System.out.println(segmentFile.getHeader().blockListSize);
        System.out.println(segmentFile.getHeader().fileLen);
        System.out.println(segmentFile.getHeader().segmentFileName);
        Assert.assertEquals(segmentFile.getHeader(), header);
    }

}
