package com.ido.robin.server.controller;

import com.ido.robin.server.SSTableManager;
import com.ido.robin.server.controller.dto.KeyDetail;
import com.ido.robin.server.util.RequestUtil;
import com.ido.robin.sstable.Block;
import com.ido.robin.sstable.KeyValue;
import com.ido.robin.sstable.SegmentFile;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2021/6/4 9:26
 */
@Slf4j
public class KeysDetailController implements RequestController {
    public static class GetKeysDetailCmd {
        public String file;
        public String pageSize = "10";
        public String page = "0";

    }

    @Override
    public HttpResponse handleInner(FullHttpRequest request) {
        GetKeysDetailCmd getCmd = RequestUtil.extractRequestParams(request, GetKeysDetailCmd.class);
        Optional<SegmentFile> t;
        t = SSTableManager.getInstance().getSegmentFiles().stream().filter(f -> f.getHeader().getSegmentFileName().equals(getCmd.file)).findFirst();

        if (t.isPresent()) {
            List<Block> result = t.get().getBlockList();
            int page = Integer.valueOf(getCmd.page);
            int pageSize = Integer.valueOf(getCmd.pageSize);
            result = result.subList(page * pageSize, page * pageSize + pageSize);
            KeyDetail keyDetail = new KeyDetail();
            keyDetail.setKeys(result.stream().map(a -> {
                KeyValue k = new KeyValue(a.getKey(), new String(a.getVal()), a.getExpiredTime());
                return k;
            }).collect(Collectors.toList()));
            keyDetail.setTotal(t.get().getBlockList().size());
            return RequestUtil.buildJsonRsp(keyDetail);
        }

        log.warn("file not found {}", getCmd.file);
        return RequestUtil.buildJsonRsp(Collections.emptyList());

    }
}
