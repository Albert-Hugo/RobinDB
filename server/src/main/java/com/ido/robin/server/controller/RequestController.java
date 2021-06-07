package com.ido.robin.server.controller;

import com.ido.robin.server.util.RequestUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:25
 */
public interface RequestController {
    default HttpResponse handle(FullHttpRequest request) {
        if ("OPTIONS".equals(request.method().name())) {
            return RequestUtil.buildJsonRsp(null);
        }
        return handleInner(request);
    }

    HttpResponse handleInner(FullHttpRequest request);
}
