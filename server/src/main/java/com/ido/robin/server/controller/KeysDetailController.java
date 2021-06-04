package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.util.RequestUtil;
import com.ido.robin.sstable.SegmentFile;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Optional;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
@Slf4j
public class KeysDetailController implements RequestController {
    public static class GetKeysDetailCmd {
        public String file;
    }

    @Override
    public HttpResponse handleRequest(FullHttpRequest request) {
        GetKeysDetailCmd getCmd = RequestUtil.extractRequestParams(request, GetKeysDetailCmd.class);
        Optional<SegmentFile> t = SSTableManager.getInstance().getSegmentFiles().stream().filter(f -> f.getHeader().getSegmentFileName().equals(getCmd.file)).findFirst();
        if (t.isPresent()) {
            return RequestUtil.buildJsonRsp(t.get().getBlockList());
        }

        log.warn("file not found {}", getCmd.file);
        return RequestUtil.buildJsonRsp(Collections.emptyList());

    }
}
