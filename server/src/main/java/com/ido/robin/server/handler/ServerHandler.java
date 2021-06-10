package com.ido.robin.server.handler;

import com.google.protobuf.ByteString;
import com.ido.robin.client.RobinClient;
import com.ido.robin.rpc.proto.RemoteCmd;
import com.ido.robin.server.SSTableManager;
import com.ido.robin.sstable.Block;
import com.ido.robin.sstable.KeyValue;
import com.ido.robin.sstable.SSTable;
import com.ido.robin.sstable.SegmentFile;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2019/1/18 11:28
 */
@Slf4j
public class ServerHandler extends SimpleChannelInboundHandler<RemoteCmd.Cmd> {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("channel {} active ", ctx.channel().id());
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemoteCmd.Cmd cmd) {
        if (cmd.getRemoteCopyRequest() != null && !cmd.getRemoteCopyRequest().getTargetHost().isEmpty()) {
            if (cmd.getRemoteCopyRequest().getType().equals(RemoteCmd.RemoteCopyRequest.CopyType.ADD)) {

                log.debug(cmd.getRemoteCopyRequest().toString());

                String host = cmd.getRemoteCopyRequest().getTargetHost();
                int port = cmd.getRemoteCopyRequest().getTargetPort();
                RobinClient client = null;
                try {
                    client = new RobinClient(host, port);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue(e.getMessage()).setId(cmd.getRemoteCopyRequest().getId()).build()).build();
                    channelHandlerContext.writeAndFlush(result);
                    return;
                }
                int start = cmd.getRemoteCopyRequest().getHashRangeStart();
                int end = cmd.getRemoteCopyRequest().getHashRangeEnd();
                // 根据hash 范围值，计算需要迁移的key value 到目标服务器
                byte[] data = calcNeedToMoveBlockData(start, end);
                client.copyData("remote.seg", data);

                //remove those key had been copy remote server
                List<Block> blocks = Block.read(data);
                List<String> toRemoveKeys = blocks.stream().map(b -> b.getKey()).collect(Collectors.toList());
                SSTableManager.getInstance().batchRemove(toRemoveKeys);

                //send response
                RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("ok").setId(cmd.getRemoteCopyRequest().getId()).build()).build();
                channelHandlerContext.writeAndFlush(result);
            } else {
                String host = cmd.getRemoteCopyRequest().getTargetHost();
                int port = cmd.getRemoteCopyRequest().getTargetPort();
                RobinClient client = null;
                try {
                    client = new RobinClient(host, port);
                } catch (Exception e) {
                    RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue(e.getMessage()).setId(cmd.getRemoteCopyRequest().getId()).build()).build();
                    channelHandlerContext.writeAndFlush(result);
                    return;
                }
                // 获取当前所有的 segment file data 复制到远程服务器
                SSTable ssTable = SSTableManager.getInstance();
                NavigableSet<Block> targetBlocks = new TreeSet<>();
                ssTable.getSegmentFiles().forEach(segmentFile -> {
                    List<Block> bs = segmentFile.getBlockList();
                    targetBlocks.addAll(bs);

                });
                byte[] data = SegmentFile.blockListToBytes(targetBlocks);
                client.copyData("remote.seg", data);

                //将当前服务中的数据拷贝完成后，删除剩余的数据，然后停止服务
                for (File f : new File(SSTableManager.getDbPath()).listFiles()) {
                    if (!f.delete()) {
                        log.error("{} file delete failed ", f.getName());
                    }
                }
                //send response
                RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("ok").setId(cmd.getRemoteCopyRequest().getId()).build()).build();
                channelHandlerContext.writeAndFlush(result).addListener((future -> {
                    if (future.isSuccess()) {
                        //退出系统
                        System.exit(0);

                    }
                }));
            }


        } else {

            switch (cmd.getBasicCmd().getType()) {
                case PUT: {
                    if (cmd.getBasicCmd().getValue().isEmpty()) {
                        return;
                    }
                    SSTableManager.getInstance().put(cmd.getBasicCmd().getKey(), cmd.getBasicCmd().getValue());
                    RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("ok").setId(cmd.getBasicCmd().getId()).build()).build();
                    channelHandlerContext.writeAndFlush(result);
                    break;
                }
                case DELETE: {
                    boolean val = SSTableManager.getInstance().remove(cmd.getBasicCmd().getKey());
                    if (val) {
                        RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("ok").setId(cmd.getBasicCmd().getId()).build()).build();
                        channelHandlerContext.writeAndFlush(result);
                    } else {
                        RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("failed").setId(cmd.getBasicCmd().getId()).build()).build();
                        channelHandlerContext.writeAndFlush(result);
                    }
                    break;
                }
                case GET: {

                    String val = SSTableManager.getInstance().get(cmd.getBasicCmd().getKey());
                    if (val == null) val = "";
                    RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue(val).setId(cmd.getBasicCmd().getId()).build()).build();
                    channelHandlerContext.writeAndFlush(result);
                    break;
                }
                case COPY: {
                    ByteString data = cmd.getBasicCmd().getData();
                    byte[] bs = data.toByteArray();
                    List<Block> blocks = Block.read(bs);
                    List<KeyValue> kvs = blocks.stream().map(b -> {
                        return new KeyValue(b.getKey(), new String(b.getVal()));
                    }).collect(Collectors.toList());
                    SSTableManager.getInstance().batchPut(kvs);
                    SSTableManager.getInstance().flush();
                    RemoteCmd.Cmd result = RemoteCmd.Cmd.newBuilder().setBasicCmd(RemoteCmd.BasicCmd.newBuilder().setValue("ok").setId(cmd.getBasicCmd().getId()).build()).build();
                    channelHandlerContext.writeAndFlush(result);
                }

            }
        }
    }

    /**
     * 根据hash 范围值，计算需要迁移的key value 到目标服务器
     *
     * @param start hash 起始
     * @param end   结束
     * @return blocks data
     */
    private byte[] calcNeedToMoveBlockData(int start, int end) {
        SSTable ssTable = SSTableManager.getInstance();
        NavigableSet<Block> targetBlocks = new TreeSet<>();
        ssTable.getSegmentFiles().forEach(segmentFile -> {
            List<Block> bs = segmentFile.getBlockList().stream().filter(block -> block.getKey().hashCode() <= end && block.getKey().hashCode() >= start).collect(Collectors.toList());
            targetBlocks.addAll(bs);

        });
        return SegmentFile.blockListToBytes(targetBlocks);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }
}
