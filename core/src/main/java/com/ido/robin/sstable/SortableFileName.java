package com.ido.robin.sstable;

/**
 * @author Ido
 * @date 2019/1/15 17:32
 */
public interface SortableFileName extends Comparable<SortableFileName> {
    String compareKey();
    /**
     * 跟key 对比之后，决定排序
     * 如果比key 小， 返回 负数， 相等 返回 0， 否则 正数
     *
     * @param key
     * @return
     */
    int compareToKey(String key);

}
