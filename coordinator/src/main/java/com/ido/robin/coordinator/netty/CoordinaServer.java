package com.ido.robin.coordinator.netty;

import com.ido.robin.coordinator.Coordinator;
import com.ido.robin.coordinator.DistributedServer;
import com.ido.robin.coordinator.DistributedWebServer;
import com.ido.robin.server.Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ido
 * @date 2019/1/18 14:01
 */
@Slf4j
public class CoordinaServer implements Server {
    private String name;
    private int port;
    private String host;

    Coordinator coordinator;

    public CoordinaServer(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void start(int port) {
        // 一个主线程组(用于监听新连接并初始化通道)，一个分发线程组(用于IO事件的处理)
        EventLoopGroup mainGroup = new NioEventLoopGroup(1);
        EventLoopGroup subGroup = new NioEventLoopGroup();
        ServerBootstrap sb = new ServerBootstrap();
        this.port = port;
        try {
            this.host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.name = sb.localAddress(port).toString();

        try {
            sb.group(mainGroup, subGroup)
                    .channel(NioServerSocketChannel.class)
                    // 这里是一个自定义的通道初始化器，用来添加编解码器和处理器
                    .childHandler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            // websocket是基于http协议的，所以需要使用http编解码器
                            pipeline.addLast(new HttpServerCodec())
                                    // 对写大数据流的支持
                                    .addLast(new ChunkedWriteHandler())
                                    // 对http消息的聚合，聚合成FullHttpRequest或FullHttpResponse
                                    // 在Netty的编程中，几乎都会使用到这个handler
                                    .addLast(new HttpObjectAggregator(1024 * 64));
                            // 以上三个处理器是对http协议的支持
                            // 自定义的处理器
                            pipeline.addLast(new CoordinatorHandler(coordinator));
                        }
                    });
            // 绑定88端口，Websocket服务器的端口就是这个
            ChannelFuture future = sb.bind(port).sync();
            log.info("coordinator server started at " + port);
            // 一直阻塞直到服务器关闭
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            mainGroup.shutdownGracefully();
            subGroup.shutdownGracefully();
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


    public static void main(String[] args) {
        List<DistributedServer> serverList = new ArrayList<>();
        serverList.add(new DistributedWebServer("test", "localhost", 8688, 8888));
//        serverList.add(new DistributedWebServer("test", "localhost", 18688, 18888));

        Coordinator coordinator = new Coordinator(serverList);
        new CoordinaServer(coordinator).start(10086);
    }
}
