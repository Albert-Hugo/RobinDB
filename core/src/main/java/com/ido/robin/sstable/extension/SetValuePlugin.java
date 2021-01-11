package com.ido.robin.sstable.extension;

import com.ido.robin.sstable.SSTable;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Ido
 * @date 2019/3/10 11:08
 */
public class SetValuePlugin extends CollectionValuePlugin {
    private SSTable ssTable;

    public SetValuePlugin(SSTable ssTable) {
        this.ssTable = ssTable;
    }

    @Override
    protected Class getCollectionClz() {
        return HashSet.class;
    }

    public Set getSet(String key) {
        byte[] bs = ssTable.getBytes(key);
        if (bs == null) return new HashSet<>();
        return (HashSet) deserialization(new String(bs), byte[].class);
    }

    /**
     * Remove a value to Set specific by the key
     *
     * @param k the key pointed to the Set
     * @param v the value to be removed
     * @return
     */
    public boolean setRemove(String k, String v) {
        Set s = getSet(k);
        s.remove(v);
        updateSet(k, s);
        return true;
    }

    /**
     * Clear the Set specific by the key
     *
     * @param k the key
     */
    public void setClear(String k) {
        Set s = getSet(k);
        s.clear();
        updateSet(k, s);
    }

    /**
     * Add a value to Set specific by the key
     *
     * @param key the key pointed to the Set
     * @param val the value to be added
     * @return
     */
    public void setAdd(String key, String val) {
        Set original = getSet(key);
        original.add(val);
        updateSet(key, original);
    }

    private void updateSet(String key, Set s) {
        ssTable.put(key, serialization(s, byte[].class));
    }


}
