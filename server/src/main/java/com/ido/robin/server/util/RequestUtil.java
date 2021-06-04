package com.ido.robin.server.util;

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ido
 * @date 2021/6/4 9:43
 */
public class RequestUtil {
    private final static Gson GSON = new Gson();

    public static String getRequestRoute(FullHttpRequest request) {
        String uri = request.uri();
        int i = uri.indexOf("?");
        if (i == -1) {
            return uri;
        }

        return uri.substring(0, i);
    }

    static String getUrlRequestParams(FullHttpRequest request) {
        String uri = request.uri();
        int i = uri.indexOf("?");
        if (i == -1) {
            return uri;
        }

        return uri.substring(i + 1);
    }


    static class KeyValua {
        String key;
        String val;

        public KeyValua(String key, String val) {
            this.key = key;
            this.val = val;
        }

    }


    public static <T> T extractRequestParams(FullHttpRequest httpRequest, Class<T> clx) {
        if (httpRequest.method().name().toLowerCase().equals("get") || httpRequest.method().name().toLowerCase().equals("delete")) {
            //url request data
            String keyValuePairs = getUrlRequestParams(httpRequest);
            List<KeyValua> kvs = toKvs(keyValuePairs);
            try {
                Object rsult = clx.newInstance();
                for (KeyValua kv : kvs) {
                    clx.getDeclaredField(kv.key).setAccessible(true);
                    clx.getDeclaredField(kv.key).set(rsult, kv.val);
                }
                return (T) rsult;
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }

            //todo
            return null;

        } else {
            //request body data
            byte[] data = new byte[httpRequest.content().readableBytes()];
            try {
                httpRequest.content().readBytes(data);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return GSON.fromJson(new String(data), clx);
        }


    }

    private static List<KeyValua> toKvs(String keyValuePairs) {
        String[] kvs = keyValuePairs.split("&");
        List result = new ArrayList();
        for (String kv : kvs) {
            String[] r = kv.split("=");
            result.add(new KeyValua(r[0], r[1]));

        }
        return result;
    }

    public static HttpResponse buildJsonRsp(Object data) {
        String content = GSON.toJson(data);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(content.getBytes());
        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().add("content-length", content.getBytes().length);
        response.headers().add("content-type", "application/json");
        //todo only for debug
        response.headers().add("Access-Control-Allow-Origin", "*");
        return response;
    }

    public static HttpResponse buildHttpRsp(String content) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(content.getBytes());
        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().add("content-length", content.getBytes().length);
        //todo only for debug
        response.headers().add("Access-Control-Allow-Origin", "*");
        return response;
    }

}
