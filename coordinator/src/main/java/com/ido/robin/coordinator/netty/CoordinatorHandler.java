package com.ido.robin.coordinator.netty;

import com.google.gson.Gson;
import com.ido.robin.coordinator.Coordinator;
import com.ido.robin.coordinator.DistributedServer;
import com.ido.robin.coordinator.DistributedWebServer;
import com.ido.robin.coordinator.dto.NodeInfo;
import com.ido.robin.server.constant.Route;
import com.ido.robin.server.controller.dto.GetCmd;
import com.ido.robin.server.controller.dto.GetKeysDetailCmd;
import com.ido.robin.server.controller.dto.RemoveCmd;
import com.ido.robin.server.util.RequestUtil;
import com.ido.robin.sstable.dto.State;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2019/8/18 10:01
 */

@Slf4j
public class CoordinatorHandler extends ChannelInboundHandlerAdapter {
    private final static Gson GSON = new Gson();

    private Coordinator coordinator;

    public CoordinatorHandler(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //普通HTTP接入
        if (msg instanceof FullHttpRequest) {
            if (((FullHttpRequest) msg).uri().equals("/favicon.ico")) {
                ctx.close();
            }
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        }
    }


    static class PutCmd {
        String key;
        String val;

        public PutCmd(String key, String val) {
            this.key = key;
            this.val = val;
        }
    }

    static class RemoveNodeCmd {
        String host;
        int port;
        int httpPort;

        public RemoveNodeCmd(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public RemoveNodeCmd(String host, int port, int httpPort) {
            this.host = host;
            this.port = port;
            this.httpPort = httpPort;
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
        CompletableFuture fsRsp = CompletableFuture.supplyAsync(() -> {
            String removeNodeCmd = "/node/delete";
            String addNodeCmd = "/node/add";
            String route = RequestUtil.getRequestRoute(request);

            if (route.equals(Route.GET)) {
                GetCmd getCmd = RequestUtil.extractRequestParams(request, GetCmd.class);
                DistributedWebServer targetServer = (DistributedWebServer) coordinator.choose(getCmd.key);
                //send request to remote server and return the response

                return RequestUtil.buildHttpRsp(new String(targetServer.get(request.uri())));

            } else if (route.equals(Route.DELETE)) {
                RemoveCmd cmd = RequestUtil.extractRequestParams(request, RemoveCmd.class);
                DistributedWebServer targetServer = (DistributedWebServer) coordinator.choose(cmd.key);
                //send request to remote server and return the response
                return RequestUtil.buildHttpRsp(new String(targetServer.delete(request.uri())));

            } else if (route.equals(Route.PUT)) {
                PutCmd cmd = RequestUtil.extractRequestParams(request, PutCmd.class);
                DistributedWebServer targetServer = (DistributedWebServer) coordinator.choose(cmd.key);
                String d = GSON.toJson(cmd);
                targetServer.put(d.getBytes());
                return RequestUtil.buildHttpRsp("ok");
            } else if (route.equals(Route.FILE_KEYS_DETAIL)) {
                GetKeysDetailCmd cmd = RequestUtil.extractRequestParams(request, GetKeysDetailCmd.class);
                //todo 获取 file 中的 start key 定位 server；
                String key = "";
                DistributedWebServer targetServer = (DistributedWebServer) coordinator.choose(key);
                String d = GSON.toJson(cmd);
                targetServer.put(d.getBytes());
                return RequestUtil.buildHttpRsp("ok");
            } else if (route.equals(Route.STATE)) {
                List<DistributedServer> serverList = coordinator.getServers();
                List<State> states = serverList.stream().map(a -> {
                    DistributedWebServer s = (DistributedWebServer) a;
                    return s.state();
                }).collect(Collectors.toList());

                return RequestUtil.buildJsonRsp(states);
            } else if (route.equals(Route.NODES_INFO)) {
                List<DistributedServer> serverList = coordinator.getServers();
                List<NodeInfo> nodeInfos = serverList.stream().map(a -> {
                    NodeInfo nodeInfo = new NodeInfo();
                    nodeInfo.healthy = a.healthy();
                    nodeInfo.host = a.host();
                    nodeInfo.port = a.port();
                    return nodeInfo;
                }).collect(Collectors.toList());

                return RequestUtil.buildJsonRsp(nodeInfos);
            } else if (request.uri().startsWith(removeNodeCmd)) {
                byte[] data = getRequestData(request);
                RemoveNodeCmd cmd = GSON.fromJson(new String(data), RemoveNodeCmd.class);

                coordinator.removeNode(cmd.host, cmd.port);
                return RequestUtil.buildHttpRsp("ok");
            } else if (request.method().name().equals("POST") && request.uri().startsWith(addNodeCmd)) {
                byte[] data = getRequestData(request);
                RemoveNodeCmd cmd = GSON.fromJson(new String(data), RemoveNodeCmd.class);

                coordinator.addNode(new DistributedWebServer("default", cmd.host, cmd.port, cmd.httpPort));
            }

            return RequestUtil.buildHttpRsp("ok");
        });

        fsRsp.thenAcceptAsync((response) -> {
            ctx.writeAndFlush(response);
            ctx.close();
            ReferenceCountUtil.release(request);
        });

    }

    private byte[] getRequestData(FullHttpRequest request) {
        byte[] data = new byte[request.content().readableBytes()];
        try {
            request.content().readBytes(data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
        return data;
    }


    /**
     * response
     *
     * @param ctx
     * @param request
     * @param response
     */
    private static void sendHttpResponse(ChannelHandlerContext ctx,
                                         FullHttpRequest request, FullHttpResponse response) {
        //返回给客户端
        if (response.status().code() != HttpResponseStatus.OK.code()) {
            ByteBuf buf = Unpooled.copiedBuffer(response.status().toString(), CharsetUtil.UTF_8);
            response.content().writeBytes(buf);
            buf.release();
        }
        //如果不是keepalive那么就关闭连接
        ChannelFuture f = ctx.channel().writeAndFlush(response);
        if (response.status().code() != HttpResponseStatus.OK.code()) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

}

