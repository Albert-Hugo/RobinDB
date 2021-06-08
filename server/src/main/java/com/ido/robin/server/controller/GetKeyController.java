package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.controller.dto.GetCmd;
import com.ido.robin.server.metrics.RequestCounterMetrics;
import com.ido.robin.server.util.RequestUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
public class GetKeyController implements RequestController {


    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        RequestCounterMetrics.inc("get");

        GetCmd getCmd = RequestUtil.extractRequestParams(request, GetCmd.class);
        String val = SSTableManager.getInstance().get(getCmd.key);

        if (val == null) {
            return RequestUtil.buildHttpRsp("");
        } else {
            return RequestUtil.buildHttpRsp(val);
        }
    }
}
