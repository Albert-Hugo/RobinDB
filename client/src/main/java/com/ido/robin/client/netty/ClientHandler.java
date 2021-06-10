package com.ido.robin.client.netty;

import com.ido.robin.rpc.proto.RemoteCmd;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * @author Ido
 * @date 2019/1/24 10:27
 */
@Slf4j
public class ClientHandler extends ChannelInboundHandlerAdapter {
    Connector connector;

    public ClientHandler(Connector that) {
        connector = that;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        connector.setServerChannel(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        RemoteCmd.Cmd cmd = (RemoteCmd.Cmd) msg;
        CountDownLatch countDownLatch = (CountDownLatch) connector.getResponseTable().get(cmd.getBasicCmd().getId());
        countDownLatch.countDown();
        //接收返回结果
        connector.getResponseTable().put(cmd.getBasicCmd().getId(), cmd.getBasicCmd().getValue());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        log.error(cause.getMessage());
    }
}
