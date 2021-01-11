package com.ido.robin.client.netty;

import com.ido.robin.rpc.proto.RemoteCmd;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.CountDownLatch;

/**
 * @author Ido
 * @date 2019/1/24 10:27
 */
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
}
