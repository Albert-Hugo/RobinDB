package com.ido.robin.sstable;

import com.ido.robin.sstable.extension.SSTablePlus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Ido
 * @date 2019/1/1 17:07
 */
@Slf4j
public class SSTableTest {
    private String key;
    private String val;
    private Random random = new Random();
    private String path = "D:\\robin-data\\";

        @Before
    public void setup() throws IOException {
        SSTable ssTable = new SSTable(path);
        for (int i = 0; i < 10; i++) {

            String k = RandomStringUtils.randomAlphanumeric(random.nextInt(5) + 20);
            String v = RandomStringUtils.randomAlphanumeric(random.nextInt(5)+random.nextInt(10));
            if (i == 5) {
                key = k;
                val = v;
                System.out.println(k);
                System.out.println(v);
            }
            ssTable.put(k, v);
        }

        ssTable.close();
    }

    @Test
    public void get() throws IOException {
        SSTable ssTable = new SSTable(path);
        System.out.println(ssTable.get("zztqbRi"));;
    }

    @Test
    public void testSearch() throws IOException {

        SSTable ssTable = new SSTable(path);
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            ssTable.get(RandomStringUtils.randomAlphanumeric(20));
            long end = System.currentTimeMillis();
            System.out.println(end - start);

        }
        Assert.assertEquals(val, ssTable.get(key));
        System.out.println(ssTable.get(key));
        ssTable.close();
    }


    @Test
    public void testSearchWithoutFlush() throws IOException {
        SSTable ssTable = new SSTable(path, false);
        String k = "safdafadada";
        String v = "2312";
        ssTable.put(k, v);
        ssTable.get(k);
        Assert.assertEquals(v, ssTable.get(k));
        ssTable.close();

    }


    @Test
    public void testPutExipred() throws IOException {
        SSTable ssTable = new SSTable(path, false);
        ssTable.put("expiredsdfs", "12", -1000);
        String result = ssTable.get("expiredsdfs");
        Assert.assertNull(result);
        ssTable.close();

    }


    @Test
    public void testExpire() throws IOException {
        SSTable ssTable = new SSTable(path, false);
        ssTable.put("expiredsdfs", "12", 1000*60);
        String result = ssTable.get("expiredsdfs");
        Assert.assertNotNull(result);
        ssTable.expire("expiredsdfs");
        Assert.assertNull(ssTable.get("expiredsdfs"));
        ssTable.close();

    }

    @Test
    public void testPut() throws IOException, InterruptedException {
//        SSTablePlus ssTable = new SSTablePlus(path);
//        ssTable.put("1","dada",10 * 1000);
//        System.out.println(ssTable.get("1"));;
        int count = 10;
        int threadN = 5;
        CountDownLatch countDownLatch = new CountDownLatch(threadN);
        SSTablePlus ssTable = new SSTablePlus(path);
        ExecutorService executorService = Executors.newCachedThreadPool();
        long start = System.currentTimeMillis();

        String key = RandomStringUtils.randomAlphanumeric(random.nextInt(20) + 1);
        String val = RandomStringUtils.randomAlphanumeric(random.nextInt(10));
        ssTable.put(key, val);
        for (int i = 0; i < threadN; i++) {

            executorService.execute(() -> {
//                List<KeyValue> list = new ArrayList<>(count);
                for (int j = 0; j < count; j++) {
                    String k = RandomStringUtils.randomAlphanumeric(random.nextInt(20) + 1);
                    String v = RandomStringUtils.randomAlphanumeric(random.nextInt(10));
//                    ssTable.put(k, v);
//                    list.listAdd(new KeyValue(k, v));
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ssTable.put(k, v);
                }
                countDownLatch.countDown();

            });
        }

        countDownLatch.await();
        long end = System.currentTimeMillis();
        log.info(String.format("flush %d record in %d , concurrent thread %d ", count * threadN, end - start, threadN));
        executorService.shutdown();
        ssTable.close();

        SSTable searchTable = new SSTable(path);
        start = System.currentTimeMillis();
        Assert.assertEquals(val, searchTable.get(key));
        end = System.currentTimeMillis();
        log.info("searching using time {}", end - start);
        searchTable.close();
    }

    @Test
    public void testAutoFlushPut() throws IOException, InterruptedException {
        int count = 10;
        int threadN = 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadN);
        SSTable ssTable = new SSTable(path, true);
        ExecutorService executorService = Executors.newCachedThreadPool();
        long start = System.currentTimeMillis();

        for (int i = 0; i < threadN; i++) {

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < count; j++) {
                        String k = RandomStringUtils.randomAlphanumeric(random.nextInt(20) + 1);
                        String v = RandomStringUtils.randomAlphanumeric(random.nextInt(2022));
                        ssTable.put(k, v);
//                    try {
//                        Thread.sleep(10);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    }
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();
        long end = System.currentTimeMillis();
        log.info(String.format("flush %d record in %d , concurrent thread %d ", count * threadN, end - start, threadN));
        executorService.shutdown();
        ssTable.close();

        Thread.sleep(1000);
        SSTable searchTable = new SSTable(path);
        start = System.currentTimeMillis();
        Assert.assertEquals(val, searchTable.get(key));
        end = System.currentTimeMillis();
        log.info("searching using time {}", end - start);
        searchTable.close();
    }


    @Test
    public void testPutSameKey() throws IOException {
        SSTable ssTable = new SSTable(path);
        String val = null;
        for (int i = 0; i < 2; i++) {
            String v = RandomStringUtils.randomAlphanumeric(random.nextInt(10));
            ssTable.put("test", v);
            System.out.println(v);
            if (i == 1) {
                val = v;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ssTable.flush();

        System.out.println(ssTable.get("test"));
        ssTable.close();

        Assert.assertEquals(val, ssTable.get("test"));

    }


    @Test
    public void testRemove() throws IOException {
        SSTable ssTable = new SSTable(path, true);
        for (int i = 0; i < 10; i++) {

            String k = RandomStringUtils.randomAlphanumeric(random.nextInt(5) + 20);
            String v = RandomStringUtils.randomAlphanumeric(random.nextInt(100));
            if (i == 5) {
                key = k;
                val = v;
                System.out.println(k);
                System.out.println(v);
            }
            ssTable.put(k, v);
        }

        ssTable.flush();

        System.out.println(ssTable.get(key));
        ssTable.remove(key);
        ssTable.flush();
        Assert.assertNull(ssTable.get(key));
        ssTable.close();

    }

    @Test
    public void testcompare() {
        System.out.println("9uPmMptvDX".compareTo("41h1TF78a3hyWyh"));
    }

    @Test
    public void testGetStateInfo() throws IOException {
        SSTable ssTable = new SSTable(path, true);
        System.out.println(ssTable.getState());
        ssTable.close();
        ;
    }

    @Test
    public void testRecovery() throws IOException {

        SSTable ssTable = new SSTable(path, false);
        System.out.println(ssTable.get(key));;

    }

}
