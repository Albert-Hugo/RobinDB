package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.metrics.RequestCounterMetrics;
import com.ido.robin.server.util.RequestUtil;
import com.ido.robin.sstable.dto.State;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
public class StateController implements RequestController {

    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        RequestCounterMetrics.inc("state");

        State val = SSTableManager.getInstance().getState();
        if (val == null) {
            return RequestUtil.buildHttpRsp("");
        } else {
            return RequestUtil.buildJsonRsp(val);
        }
    }
}
