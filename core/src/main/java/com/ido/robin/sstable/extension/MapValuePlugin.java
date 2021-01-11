package com.ido.robin.sstable.extension;

import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.serializers.MapSerializer;
import com.ido.robin.sstable.SSTable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ido
 * @date 2019/3/10 11:08
 */
public class MapValuePlugin extends CollectionValuePlugin {
    private SSTable ssTable;

    public MapValuePlugin(SSTable ssTable) {
        this.ssTable = ssTable;
    }

    @Override
    protected Serializer getSerializer() {
        return new MapSerializer();
    }


    @Override
    protected Class getCollectionClz() {
        return HashMap.class;
    }

    /**
     * get a hash map by key
     *
     * @param key the key
     * @return the target hash map
     */
    public Map getMap(String key) {
        byte[] bs = ssTable.getBytes(key);
        if (bs == null) return new HashMap();
        return (Map) deserialization(new String(bs), byte[].class);
    }

    /**
     * Add to key value pairs to specific key's  hash map
     *
     * @param key   the key pointed to hash map
     * @param hashK the key to store in hash map
     * @param hashV the value to store in hash map
     */
    public void mapAdd(String key, String hashK, String hashV) {
        Map original = getMap(key);
        original.put(hashK, hashV);
        update(key, original);
    }

    /**
     * Clear the hash map specific by the key
     *
     * @param key the key
     */
    public void mapClear(String key) {
        Map original = getMap(key);
        original.clear();
        update(key, original);
    }

    /**
     * remove a key value from the hash map specific by the key
     *
     * @param key   the key pointed to hash map
     * @param hashK the key in hash map to be removed
     */
    public void mapRemove(String key, String hashK) {
        Map original = getMap(key);
        original.remove(hashK);
        update(key, original);
    }

    private void update(String k, Map m) {
        ssTable.put(k, serialization(m, byte[].class));
    }

}
