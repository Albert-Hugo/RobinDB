package com.ido.robin.server.controller;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:25
 */
public interface RequestController {

    HttpResponse handleRequest(FullHttpRequest request);

}
