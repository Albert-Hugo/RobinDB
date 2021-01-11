package com.ido.robin.sstable;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author Ido
 * @date 2019/1/1 14:43
 */
public class SparseIndexMemTest {
    String key = null;
    String val = null;
    private static String path = "D:\\robin-data\\";
    private String removeKey;
    private String removeVel;
    String fileName = null;
    private Random random = new Random();

    @Before
    public void setup() {

        try {
            SegmentFile segmentFile = new SegmentFile(path + "test.seg");
            for (int i = 0; i < 10; i++) {
                String k = RandomStringUtils.randomAlphanumeric(random.nextInt(5) + 20);
                String v = RandomStringUtils.randomAlphanumeric(random.nextInt(256) + 1);
                if (i == 0) {
                    key = k;
                    val = v;
                    System.out.println(k);
                    System.out.println(v);
                }

                if (i == 5) {
                    removeKey = k;
                    removeVel = v;
                    System.out.println(removeKey);
                    System.out.println(removeVel);
                }
                if (i == 2) {
                    segmentFile.put("expired", v.getBytes(),- 1000);
                } else {

                    segmentFile.put(k, v.getBytes());
                }
            }
            segmentFile.flush();
            fileName = segmentFile.getOriginalFileName();
            System.out.println("new file name " + fileName);
            Assert.assertEquals(val, new String(segmentFile.get(key).val));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSearch() throws IOException {
        SparseIndexMem indexMem = new SparseIndexMem(Arrays.asList(new SegmentFile(fileName)));
        //CjG9sxCK JR79myW6iBjgV8f6XjfH
        Assert.assertEquals(val, new String(indexMem.search(key).getVal()));
    }

    @Test
    public void testSearchExpired() throws IOException {
        SparseIndexMem indexMem = new SparseIndexMem(Arrays.asList(new SegmentFile(fileName)));
        //CjG9sxCK JR79myW6iBjgV8f6XjfH
        Assert.assertNull(indexMem.search("expired"));
    }

//    @Test
//    public void testRemove() throws IOException {
//        SparseIndexMem indexMem = new SparseIndexMem(Arrays.asList(new SegmentFile(fileName)));
//        indexMem.remove(removeKey);
//        Assert.assertNull(indexMem.search(removeKey));
//    }
}
