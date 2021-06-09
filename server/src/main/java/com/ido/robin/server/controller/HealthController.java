package com.ido.robin.server.controller;

import com.ido.robin.server.util.RequestUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
public class HealthController implements RequestController {


    @Override
    public HttpResponse handleInner(FullHttpRequest request) {

        return RequestUtil.buildHttpRsp("ok");
    }
}
