package com.ido.robin.server.handler;

import com.google.gson.Gson;
import com.ido.robin.server.SSTableManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * @author Ido
 * @date 2019/8/18 10:01
 */

@Slf4j
public class WebServerHandler extends ChannelInboundHandlerAdapter {
    private final static Gson GSON = new Gson();


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

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
        CompletableFuture fsRsp = CompletableFuture.supplyAsync(() -> {
            String getCmd = "/get/";
            String deleteCmd = "/delete/";
            String putCmd = "/put";
            if (request.method().name().equals("GET") && request.uri().startsWith(getCmd)) {
                int i = request.uri().indexOf(getCmd);
                String k = request.uri().substring(i + getCmd.length());
                String val = SSTableManager.getInstance().get(k);
                if (val == null) {
                    return buildHttpRsp("");
                } else {
                    return buildHttpRsp(val);
                }

            } else if (request.method().name().equals("DELETE") && request.uri().startsWith(deleteCmd)) {
                int i = request.uri().indexOf(deleteCmd);
                String k = request.uri().substring(i + deleteCmd.length());
                SSTableManager.getInstance().remove(k);
                return buildHttpRsp("ok");

            } else if (request.method().name().equals("POST") && request.uri().startsWith(putCmd)) {
                byte[] data = new byte[request.content().readableBytes()];
                try {
                    request.content().readBytes(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                PutCmd cmd = GSON.fromJson(new String(data), PutCmd.class);
                SSTableManager.getInstance().put(cmd.key, cmd.val);
                return buildHttpRsp("ok");
            }

            return null;
        });

        fsRsp.thenAcceptAsync((response) -> {
            ctx.writeAndFlush(response);
            ReferenceCountUtil.release(request);
        });

    }

    private HttpResponse buildHttpRsp(String content) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(content.getBytes());
        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().add("content-length", content.getBytes().length);
        return response;
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

