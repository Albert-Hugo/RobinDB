package com.ido.robin.sstable.extension;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.util.IOUtils;
import com.ido.robin.sstable.Block;
import com.ido.robin.sstable.SSTable;

/**
 * @author Ido
 * @date 2020/12/22 16:36
 */
public class JsonValuePlugin {
    private SSTable ssTable;
    private static final ParserConfig DEFAULT_FAST_JSON_CONFIG = new ParserConfig();

    static {
        DEFAULT_FAST_JSON_CONFIG.setAutoTypeSupport(true);
    }


    public JsonValuePlugin(SSTable ssTable) {
        this.ssTable = ssTable;
    }

    public void put(String key, Object val, long expiredTime) {
        byte[] bytes = JSON.toJSONBytes(val, new SerializerFeature[]{SerializerFeature.WriteClassName});
        this.ssTable.put(key, new String(bytes), expiredTime);
    }

    public void put(String key, Object val) {
        this.put(key, val, Block.PERMANENT);
    }


    public Object get(String key) {
        String val = this.ssTable.get(key);
        if (val == null) {
            return null;
        }

        byte[] bytes = val.getBytes();
        if (bytes.length != 0) {
            try {
                return JSON.parseObject(new String(bytes, IOUtils.UTF8), Object.class, DEFAULT_FAST_JSON_CONFIG, Feature.AutoCloseSource);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Could not deserialize: " + e.getMessage(), e);
            }
        } else {
            return null;
        }

    }
}
