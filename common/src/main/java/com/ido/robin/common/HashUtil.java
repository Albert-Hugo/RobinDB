package com.ido.robin.common;

/**
 * @author Ido
 * @date 2021/6/10 16:49
 */
public class HashUtil {
    public static int hash(String k) {
        return Math.abs(k.hashCode());
    }
}
