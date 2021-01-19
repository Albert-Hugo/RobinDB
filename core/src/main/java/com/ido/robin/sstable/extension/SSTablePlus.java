package com.ido.robin.sstable.extension;

import com.ido.robin.sstable.SSTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ido
 * @date 2019/3/9 20:41
 */
public class SSTablePlus extends SSTable {

    private JsonValuePlugin jsonValuePlugin;


    public SSTablePlus(String path) throws IOException {
        super(path);
        jsonValuePlugin = new JsonValuePlugin(this);
    }

    public SSTablePlus(String path, boolean flushEveryTime) throws IOException {
        super(path, flushEveryTime);
    }

    public List getList(String key) {
        List<Object> list = getObject(key);
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    public void listAdd(String key, String val) {

        List l = getList(key);
        l.add(val);
        putObject(key,l);
        flush();
    }

    public void listRemove(String key, String val) {
        List l = getList(key);
        l.remove(val);
        putObject(key,l);
        flush();
    }

    public Set getSet(String key) {
        Set<Object> set = getObject(key);
        if (set == null) {
            set = new HashSet<>();
        }
        return set;
    }

    public boolean setRemove(String k, String v) {
        Set set = getSet(k);
        boolean result = set.remove(v);
        putObject(k, set);
        flush();
        return result;
    }

    public void setClear(String k) {
        remove(k);
        flush();

    }

    public void setAdd(String key, String val) {
        Set<Object> set = getObject(key);
        if (set == null) {
            set = new HashSet<>();
        }
        set.add(val);
        putObject(key, set);
        flush();
    }

    public Map getMap(String key) {
        Map<String, Object> map = getObject(key);
        if (map == null) {
            map = new HashMap<>();
        }
        return map;
    }

    public void mapAdd(String key, String hashK, String hashV) {
        Map<String, Object> map = getObject(key);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(hashK, hashV);
        putObject(key, map);
        flush();
    }

    public void mapClear(String key) {
       Map map = getMap(key);
       map.clear();
       putObject(key,map);
       flush();
    }

    public void mapRemove(String key, String hashK) {
        Map map = getMap(key);
        map.remove(hashK);
        putObject(key,map);
        flush();
    }

    public void putObject(String key, Object val) {
        jsonValuePlugin.put(key, val);
    }

    public void putObject(String key, Object val, long expiredTime) {
        jsonValuePlugin.put(key, val, expiredTime);
    }

    public <T> T getObject(String key) {
        return (T) jsonValuePlugin.get(key);
    }
}
