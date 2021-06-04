package com.ido.robin.server.util;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Ido
 * @date 2021/6/4 10:03
 */
public class RequestUtilTest {

    @Test
    public void testGetUrlRequestParams(){
        String params = RequestUtil.getUrlRequestParams(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,"/get?key=1"));

        Assert.assertEquals("key=1",params);
    }
}
