package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.controller.dto.PutCmd;
import com.ido.robin.server.metrics.RequestCounterMetrics;
import com.ido.robin.server.util.RequestUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
@Slf4j
public class PutKeyController implements RequestController {

    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        RequestCounterMetrics.inc("put-key");

        PutCmd cmd = RequestUtil.extractRequestParams(request, PutCmd.class);
        log.info("put key :{},val :{}", cmd.key, cmd.val);
        SSTableManager.getInstance().put(cmd.key, cmd.val, cmd.expiredTime);
        return RequestUtil.buildHttpRsp("ok");
    }
}
