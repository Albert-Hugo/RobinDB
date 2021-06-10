package com.ido.robin.client.netty;

import com.google.protobuf.ByteString;
import com.ido.robin.client.RemoteCopyException;
import com.ido.robin.rpc.proto.RemoteCmd;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ido
 * @date 2019/1/24 10:24
 */
@Slf4j
public class Connector {
    NioEventLoopGroup nioEventLoopGroup;
    private String host;
    private int port;
    private Channel serverChannel;
    private AtomicInteger cmdId = new AtomicInteger(0);
    private ConcurrentHashMap responseTable = new ConcurrentHashMap<>();

    public Connector(String host, int port) {
        this.host = host;
        this.port = port;
    }

    ConcurrentHashMap getResponseTable() {
        return responseTable;
    }

    Connector setResponseTable(ConcurrentHashMap responseTable) {
        this.responseTable = responseTable;
        return this;
    }

    Channel getServerChannel() {
        return serverChannel;
    }

    Connector setServerChannel(Channel serverChannel) {
        this.serverChannel = serverChannel;
        return this;
    }

    public void connect() throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        nioEventLoopGroup = new NioEventLoopGroup();
        Connector that = this;
        bootstrap.group(nioEventLoopGroup)
                .channel(NioSocketChannel.class)
//                .option(ChannelOption.SO_BACKLOG, 100)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline
                                .addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder())
                                .addLast(new ProtobufDecoder(RemoteCmd.Cmd.getDefaultInstance()))
                                .addLast(new ClientHandler(that));
                    }
                });
        ChannelFuture f = bootstrap.connect(host, port);

        try {
            f.sync();
        } catch (Exception e) {
            try {
                throw e;
            } catch (InterruptedException e1) {
                log.error(e.getMessage(), e1);
            }
        }

        f.channel().closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    log.info(" connection close");
                }
            }
        });

    }

    public void put(String k, String v) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteCmd.Cmd cmd = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setId(cmdId.getAndIncrement()).setKey(k).setValue(v).setType(RemoteCmd.BasicCmd.CmdType.PUT).build()).build();
        responseTable.put(cmd.getBasicCmd().getId(), countDownLatch);
        this.serverChannel.writeAndFlush(cmd).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {

                    log.debug("send put cmd success");
                }
            }
        });
        try {
            countDownLatch.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void sendRemoteCopyRequest(String host, int port, int start, int end, RemoteCmd.RemoteCopyRequest.CopyType type) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteCmd.Cmd cmd = RemoteCmd.Cmd.newBuilder().setRemoteCopyRequest(RemoteCmd.RemoteCopyRequest.newBuilder()
                .setId(cmdId.getAndIncrement())
                .setTargetHost(host)
                .setTargetPort(port)
                .setHashRangeEnd(end)
                .setHashRangeStart(start)
                .setType(type)
                .build())
                .build();
        responseTable.put(cmd.getRemoteCopyRequest().getId(), countDownLatch);
        this.serverChannel.writeAndFlush(cmd).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("send put cmd success");
            }
        });
        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        /**
         * 返回结果在 {@link ClientHandler#channelRead(ChannelHandlerContext, Object)} 中设置
         */
        String result = (String) responseTable.get(cmd.getRemoteCopyRequest().getId());
        if (!"ok".equals(result)) {
            throw new RemoteCopyException(result);
        }
        log.info("result : [{}]", result);
    }

    /**
     * 远程拷贝，用于集群数据的迁移
     *
     * @param fileName 文件名
     * @param data     数据内容
     */
    public void copy(String fileName, byte[] data) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteCmd.Cmd cmd = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder()
                .setId(cmdId.getAndIncrement())
                .setData(ByteString.copyFrom(data))
                .setFileName(fileName)
                .setType(RemoteCmd.BasicCmd.CmdType.COPY)
                .build())
                .build();
        responseTable.put(cmd.getBasicCmd().getId(), countDownLatch);
        this.serverChannel.writeAndFlush(cmd).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("send put cmd success");
            }
        });
        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void delete(String k) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteCmd.Cmd cmd = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setId(cmdId.getAndIncrement()).setKey(k).setType(RemoteCmd.BasicCmd.CmdType.DELETE).build()).build();
        responseTable.put(cmd.getBasicCmd().getId(), countDownLatch);
        this.serverChannel.writeAndFlush(cmd).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("send put cmd success");
            }
        });
        try {
            countDownLatch.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String get(String k) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteCmd.Cmd cmd = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setId(cmdId.getAndIncrement()).setKey(k).setType(RemoteCmd.BasicCmd.CmdType.GET).build()).build();
        responseTable.put(cmd.getBasicCmd().getId(), countDownLatch);
        this.serverChannel.writeAndFlush(cmd).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    log.debug("send cmd success");

                }
            }
        });
        try {
            countDownLatch.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        String rspf = (String) responseTable.get(cmd.getBasicCmd().getId());

        return rspf;

    }

    public void close() {
        nioEventLoopGroup.shutdownGracefully();
    }
}
