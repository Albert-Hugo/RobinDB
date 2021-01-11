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
    private static String path = "D:\\生产已经存在的服务评价\\";


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
//
//        ssTablePlus.putObject("person", new Person("123"));
//        Set<Person> personSet = Sets.newHashSet(new Person("123"));
//        ssTablePlus.putObject("list", Arrays.asList("123","2314"));
//        ssTablePlus.putObject("set", personSet);
//        Map m  = Maps.newHashMap();
//        m.put("test","fsf");
//        ssTablePlus.putObject("map", m);
//
//        ssTablePlus.flush();
//        Person p = ssTablePlus.getObject("person");
//        List list = ssTablePlus.getObject("list");
        Set set = ssTablePlus.getObject("salesOrder");
//        Map map = ssTablePlus.getObject("map");

//        Assert.assertEquals("123", p.name);
//        Assert.assertEquals(2, list.size());
        Assert.assertEquals(2, set.size());
//        Assert.assertEquals("fsf", map.get("test"));
    }
}
