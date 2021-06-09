package com.ido.robin.coordinator;

import com.ido.robin.client.RobinClient;
import com.ido.robin.common.HttpUtil;
import com.ido.robin.rpc.proto.RemoteCmd;
import com.ido.robin.server.constant.Route;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * @author Ido
 * @date 2019/2/15 9:42
 */
@Slf4j
public class Coordinator {
    List<DistributedServer> servers;
    private HashRing hashRing;

    public Coordinator(List<DistributedServer> servers) {
        if (servers == null || servers.isEmpty()) {
            throw new IllegalStateException("servers list can not be empty");
        }
        this.servers = servers;
        hashRing = new HashRing(servers.size());
        this.servers.forEach(s -> {
            s.setSlot(hashRing.locateSlot(s));
        });

        monitorServersHealth();

    }

    private void monitorServersHealth() {
        for (int i = 0; i < this.servers.size(); i++) {
            DistributedServer s = servers.get(i);
            Thread thread = new Thread(() -> {
                int sleepInterval = Integer.getInteger("health.interval", 5000);
                int maxInterval = Integer.getInteger("health.max.interval", 60 * 1000);
                while (true) {
                    String url = "http://" + s.host() + ":" + s.getHttpPort() + "/" + Route.HEALTH;
                    byte[] bs = HttpUtil.get(url, null);
                    if (bs != null && "ok".equals(new String(bs))) {
                        s.setHealth(true);
                        sleepInterval = 5000;
                    } else {
                        log.warn("host：{} not healthy", s.host());
                        //设置最大请求间隔60s，如果一直出错，时间翻倍递增

                        sleepInterval = sleepInterval * 2 > maxInterval ? maxInterval : sleepInterval * 2;
                        s.setHealth(false);
                    }


                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                        Thread.interrupted();
                    }
                }


            });
            thread.setDaemon(true);
            thread.start();
        }

    }


    public List<DistributedServer> getServers() {
        return this.servers;
    }

    /**
     * 根据key 获取指定的server
     *
     * @param key 待操作的key
     * @return 被选中的server
     */
    public DistributedServer choose(String key) {
        //todo for not healthy node , not to return or duplicate data to other nodes.
        int hashVal = hash(key);
        Optional<DistributedServer> s = servers.stream().filter(server -> server.isInRange(hashVal)).findFirst();
        if (!s.isPresent()) {
            throw new IllegalStateException("server not found");
        }
        return s.get();
    }

    /**
     * 移除指定的server 节点
     *
     * @param host
     * @param port
     * @return
     */
    public DistributedServer removeNode(String host, int port) {
        //获取被移除server 的下一个节点，并将下一个节点的hash range 包含被移除的节点范围。同时将被移除的节点的文件迁移到下一个节点上
        Optional<DistributedServer> p = this.servers.stream().filter(s -> s.host().equals(host) && s.port() == port).findAny();
        if (!p.isPresent()) {
            log.info("node not found for {}:{}", host, port);
            return null;
        }
        DistributedServer toRemove = p.get();
        DistributedServer nextNode = getNextNode(toRemove);
        HashRing.Slot mergedSlot = this.hashRing.removeSlot(toRemove.getSlot());
        nextNode.setSlot(mergedSlot);
        remoteCopy(toRemove, nextNode, RemoteCmd.RemoteCopyRequest.CopyType.REMOVE);

        return toRemove;
    }


    private DistributedServer getNextNode(DistributedServer toRemove) {
        if (toRemove.rangeEnd() == HashRing.getMaxRange()) {
            return this.servers.stream().filter(s -> s.rangeStart() == 0).findFirst().get();
        } else {
            return this.servers.stream().filter(s -> {
                return s.rangeStart() == toRemove.rangeEnd();
            }).findAny().orElse(null);

        }

    }

    /**
     * 添加server 到 集群列表
     *
     * @param server 待添加server
     * @return server name
     */
    public String addNode(DistributedServer server) {
        //如果哈希环上没有足够的空闲槽位，则添加
        HashRing.Slot removedSlot = null;
        if (this.hashRing.emptySlots().size() == 0) {
            removedSlot = this.hashRing.addSlot();
        }
        //被新节点隔断的hash 值重新分配到新的节点上。 需要进行remote copy
        DistributedServer source = null;
        for (DistributedServer s : servers) {
            if (s.includeSlot(removedSlot)) {
                //如果server 被新分配的slot 截断，则重新分配slot
                s.setSlot(hashRing.locateSlot(s));
                //获取被截断的server
                source = s;
                break;
            }
        }
        server.setSlot(hashRing.locateSlot(server));

        this.servers.add(server);

        remoteCopy(source, server, RemoteCmd.RemoteCopyRequest.CopyType.ADD);

        return server.name();
    }

    /**
     * @param source the source server
     * @param target target server
     */
    void remoteCopy(DistributedServer source, DistributedServer target, RemoteCmd.RemoteCopyRequest.CopyType type) {
        //将文件直接从源服务器迁移到 目标服务器，减少不必要的拷贝
        String host = source.host();
        int port = source.port();
        target.rangeStart();
        RobinClient client = new RobinClient(host, port);
        client.sendRemoteCopyRequest(target.host(), target.port(), target.rangeStart(), target.rangeEnd(), type);


    }


    private int hash(String key) {
        return Math.abs(key.hashCode());
    }

}
