package com.ido.robin.common;

import com.google.gson.Gson;

/**
 * @author Ido
 * @date 2021/6/8 16:55
 */
public class JsonUtil {
    private static final Gson gson = new Gson();

    public static String toJson(Object o) {
        return gson.toJson(o);
    }


    public static <T> T fromJson(String json, Class<T> clx) {
        return gson.fromJson(json, clx);
    }
}
