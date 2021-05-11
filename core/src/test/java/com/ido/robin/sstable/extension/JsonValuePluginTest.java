package com.ido.robin.sstable.extension;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ido
 * @date 2020/12/22 16:57
 */
public class JsonValuePluginTest {
    private static String path = "D:\\robin-data";


    public static class Person {
        private String name;

        public String getName() {
            return name;
        }

        public Person setName(String name) {
            this.name = name;
            return this;
        }

        public Person() {
        }

        public Person(String name) {
            this.name = name;
        }
    }

    @Test
    public void testPutObject() throws IOException {
        SSTablePlus ssTablePlus = new SSTablePlus(path);
        Person  p = new Person();
        p.setName("a");
        ssTablePlus.putObject("fs",p);
//
        Person pp = ssTablePlus.getObject("fs");

        Assert.assertEquals("a", pp.getName());
    }
}
