package com.ido.robin.server.controller;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
public class NotFoundController implements RequestController {
    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer("".getBytes());
        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, byteBuf);
        response.headers().add("content-length", 0);
        return response;
    }
}
