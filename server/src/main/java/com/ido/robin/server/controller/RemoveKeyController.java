package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.controller.dto.RemoveCmd;
import com.ido.robin.server.metrics.RequestCounterMetrics;
import com.ido.robin.server.util.RequestUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
public class RemoveKeyController implements RequestController {

    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        RequestCounterMetrics.inc("delete-key");
        RemoveCmd cmd = RequestUtil.extractRequestParams(request, RemoveCmd.class);

        SSTableManager.getInstance().remove(cmd.key);
        return RequestUtil.buildHttpRsp("ok");
    }
}
