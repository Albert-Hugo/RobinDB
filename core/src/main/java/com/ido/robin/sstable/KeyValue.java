package com.ido.robin.sstable;

/**
 * key value pairs
 *
 * @author Ido
 * @date 2019/1/25 15:55
 */
public class KeyValue {
    private String key;
    private String val;
    private long expiredTime  = Block.PERMANENT;

    public KeyValue(String key, String val, long expiredTime) {
        this.key = key;
        this.val = val;
        if (expiredTime != Block.PERMANENT) {
            this.expiredTime = System.currentTimeMillis() + expiredTime;
        }
    }

    public KeyValue(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public KeyValue setKey(String key) {
        this.key = key;
        return this;
    }

    public String getVal() {
        return val;
    }

    public KeyValue setVal(String val) {
        this.val = val;
        return this;
    }

    public long getExpiredTime() {
        return expiredTime;
    }

    public KeyValue setExpiredTime(long expiredTime) {
        this.expiredTime = expiredTime;
        return this;
    }
}
