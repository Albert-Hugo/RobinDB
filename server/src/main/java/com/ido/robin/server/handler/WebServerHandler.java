package com.ido.robin.server.handler;

import com.ido.robin.server.constant.Route;
import com.ido.robin.server.controller.GetKeyController;
import com.ido.robin.server.controller.HealthController;
import com.ido.robin.server.controller.KeysDetailController;
import com.ido.robin.server.controller.NotFoundController;
import com.ido.robin.server.controller.PutKeyController;
import com.ido.robin.server.controller.RemoveKeyController;
import com.ido.robin.server.controller.RequestController;
import com.ido.robin.server.controller.StateController;
import com.ido.robin.server.util.RequestUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * @author Ido
 * @date 2019/8/18 10:01
 */

@Slf4j
public class WebServerHandler extends ChannelInboundHandlerAdapter {
    private static Map<String, RequestController> handlersMapping = new HashMap<>();
    private final NotFoundController notFoundController = new NotFoundController();

    static {
        handlersMapping.put(Route.GET, new GetKeyController());
        handlersMapping.put(Route.HEALTH, new HealthController());
        handlersMapping.put(Route.DELETE, new RemoveKeyController());
        handlersMapping.put(Route.PUT, new PutKeyController());
        handlersMapping.put(Route.STATE, new StateController());
        handlersMapping.put(Route.FILE_KEYS_DETAIL, new KeysDetailController());
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


    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
        log.info(request.uri());

        CompletableFuture fsRsp = CompletableFuture.supplyAsync(() -> {
            String route = RequestUtil.getRequestRoute(request);
            RequestController controller = handlersMapping.get(route);
            if (controller != null) {
                try {

                    return controller.handle(request);
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                    return (RequestController) request1 -> RequestUtil.buildHttpRsp(ex.getMessage());
                }
            }

            return notFoundController.handle(request);
        }, Executors.newFixedThreadPool(100));

        fsRsp.thenAcceptAsync((response) -> {
            ctx.writeAndFlush(response);
            ReferenceCountUtil.release(request);
        });

    }


}

