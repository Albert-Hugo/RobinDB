package com.ido.robin.sstable.extension;

import com.ido.robin.sstable.SSTable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ido
 * @date 2019/3/9 20:41
 */
public class SSTablePlus extends SSTable {

    private ListValuePlugin listValuePlugin;
    private MapValuePlugin mapValuePlugin;
    private SetValuePlugin setValuePlugin;
    private JsonValuePlugin jsonValuePlugin;


    public SSTablePlus(String path) throws IOException {
        super(path);
        listValuePlugin = new ListValuePlugin(this);
        mapValuePlugin = new MapValuePlugin(this);
        setValuePlugin = new SetValuePlugin(this);
        jsonValuePlugin = new JsonValuePlugin(this);
    }

    public SSTablePlus(String path, boolean flushEveryTime) throws IOException {
        super(path, flushEveryTime);
        listValuePlugin = new ListValuePlugin(this);
        mapValuePlugin = new MapValuePlugin(this);
        setValuePlugin = new SetValuePlugin(this);
    }

    public List getList(String key) {
        return listValuePlugin.getList(key);
    }

    public <T> Map<String, List<T>> getAllList(Class<T> targetClz) {
        return listValuePlugin.getAllList(targetClz);
    }

    public Map<String, List<Object>> getAllList() {
        return listValuePlugin.getAllList();
    }

    public void listAdd(String key, byte[] val) {
        listValuePlugin.listAdd(key, val);
    }

    public void listRemove(String key, byte[] val) {
        listValuePlugin.listRemove(key, val);
    }

    public Set getSet(String key) {
        return setValuePlugin.getSet(key);
    }

    public boolean setRemove(String k, String v) {
        return setValuePlugin.setRemove(k, v);
    }

    public void setClear(String k) {
        setValuePlugin.setClear(k);
    }

    public void setAdd(String key, String val) {
        setValuePlugin.setAdd(key, val);
    }

    public Map getMap(String key) {
        return mapValuePlugin.getMap(key);
    }

    public void mapAdd(String key, String hashK, String hashV) {
        mapValuePlugin.mapAdd(key, hashK, hashV);
    }

    public void mapClear(String key) {
        mapValuePlugin.mapClear(key);
    }

    public void mapRemove(String key, String hashK) {
        mapValuePlugin.mapRemove(key, hashK);
    }

    public void putObject(String key, Object val) {
        jsonValuePlugin.put(key, val);
    }

    public void putObject(String key, Object val,long expiredTime) {
        jsonValuePlugin.put(key, val,expiredTime);
    }

    public <T> T getObject(String key) {
        return (T) jsonValuePlugin.get(key);
    }
}
