package com.ido.robin.coordinator;

/**
 * 支持在哈希环上定位
 *
 * @author Ido
 * @date 2019/2/15 9:53
 */
public interface coordinable {
    /**
     * 哈希环上的起始值
     *
     * @return
     */
    int rangeStart();

    /**
     * 哈希环上的结束值
     *
     * @return
     */
    int rangeEnd();

    /**
     * 哈希值在当前server 的范围内
     *
     * @param hashVal
     * @return
     */
    default boolean isInRange(int hashVal) {
        return rangeStart() <= hashVal && hashVal <= rangeEnd();
    }


}
