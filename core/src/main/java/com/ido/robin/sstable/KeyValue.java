package com.ido.robin.sstable;

import lombok.Data;

/**
 * key value pairs
 *
 * @author Ido
 * @date 2019/1/25 15:55
 */
@Data
public class KeyValue {
    private String key;
    private String val;
    private long expiredTime = Block.PERMANENT;

    public KeyValue(String key, String val, long expiredTime) {
        this.key = key;
        this.val = val;
        this.expiredTime = expiredTime;
    }

    public KeyValue(String key, String val) {
        this.key = key;
        this.val = val;
    }

}
