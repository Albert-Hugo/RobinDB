package com.ido.robin.server;


import com.ido.robin.rpc.proto.RemoteCmd;
import com.ido.robin.server.handler.ServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Ido
 * @date 2019/1/18 11:10
 */
@Slf4j
public class ServerStarter implements Server {
    private String name;
    private int port;
    private String host;

    public void start(int port) throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup master = new NioEventLoopGroup();
        EventLoopGroup worker = new NioEventLoopGroup();
        this.port = port;
        try {
            this.host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.name = bootstrap.localAddress(port).toString();
        try {

            bootstrap.group(master, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline
                                    .addLast(new ProtobufVarint32FrameDecoder())
                                    .addLast(new ProtobufVarint32LengthFieldPrepender())
                                    .addLast(new ProtobufEncoder())
                                    .addLast(new ProtobufDecoder(RemoteCmd.Cmd.getDefaultInstance()))
                                    .addLast(new ServerHandler())
                            ;
                        }
                    });
            ChannelFuture f = bootstrap.bind(port);

            f
                    .addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                future.cause().printStackTrace();
                                log.error(future.cause().getMessage(), future.cause());
                                return;
                            }

                            log.info("RobinDB server started at " + port);


                        }
                    });


            f.channel().closeFuture().sync();

        } finally {
            log.info("client shutdown");
            master.shutdownGracefully();
            worker.shutdownGracefully();


        }
    }


    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String host() {
        return this.host;
    }

    @Override
    public int port() {
        return this.port;
    }
}