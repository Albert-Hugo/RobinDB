package com.ido.robin.sstable.extension;

import com.google.gson.Gson;
import com.ido.robin.sstable.Block;
import com.ido.robin.sstable.SSTable;
import com.ido.robin.sstable.SegmentFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ido
 * @date 2019/3/10 11:08
 */
public class ListValuePlugin extends CollectionValuePlugin {
    private final Gson gson = new Gson();
    private SSTable ssTable;


    public ListValuePlugin(SSTable ssTable) {
        this.ssTable = ssTable;
    }

    @Override
    protected Class getCollectionClz() {
        return ArrayList.class;
    }

    public List getList(String key) {
        byte[] bs = ssTable.getBytes(key);
        if (bs == null) return new ArrayList();
        return (ArrayList) deserialization(new String(bs), byte[].class);
    }

    /**
     * get all the result list from data file
     *
     * @param targetClz the target result class to convert
     * @param <T>
     * @return
     */
    public <T> Map<String, List<T>> getAllList(Class<T> targetClz) {
        Map result = new HashMap();

        for (SegmentFile sg : ssTable.getSegmentFiles()) {
            for (Block b : sg.getBlockList()) {
                List data = (ArrayList) deserialization(new String(b.getVal()), byte[].class);
                if (targetClz != null) {
                    List<T> reulstList = new ArrayList<>(data.size());
                    for (Object o : data) {
                        reulstList.add(gson.fromJson(new String((byte[]) o), targetClz));
                    }
                    result.put(b.getKey(), reulstList);
                } else {
                    result.put(b.getKey(), data);
                }
            }
        }
        return result;
    }

    public Map<String, List<Object>> getAllList() {
        return getAllList(null);
    }

    /**
     * Add a value to List specific by the key
     *
     * @param key the key pointed to the List
     * @param val the value to be added
     */
    public void listAdd(String key, byte[] val) {
        List original = getList(key);
        original.add(val);
        update(key, original);
    }


    /**
     * remove a value from List
     *
     * @param key the key pointed to the List
     * @param val the value to be removed
     */
    public void listRemove(String key, byte[] val) {
        List original = getList(key);
        original.remove(val);
        update(key, original);
    }

    private void update(String k, List original) {
        ssTable.put(k, serialization(original, byte[].class));
    }
}
