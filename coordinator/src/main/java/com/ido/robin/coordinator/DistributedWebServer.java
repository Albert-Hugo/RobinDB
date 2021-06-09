package com.ido.robin.coordinator;

import com.ido.robin.common.HttpUtil;
import com.ido.robin.common.JsonUtil;
import com.ido.robin.server.constant.Route;
import com.ido.robin.server.controller.dto.KeyDetail;
import com.ido.robin.server.controller.dto.PutCmd;
import com.ido.robin.server.controller.dto.RemoveCmd;
import com.ido.robin.sstable.dto.State;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author Ido
 * @date 2019/2/15 10:20
 */
public class DistributedWebServer implements DistributedServer {
    private HashRing.Slot slot;
    final private String host;
    final private int port;
    final private int httpPort;
    final private String name;

    /**
     * @param name the server name
     * @param host the host
     * @param port the port
     */
    public DistributedWebServer(String name, String host, int port, int httpPort) {
        this.host = host;
        this.port = port;
        this.httpPort = httpPort;
        this.name = name;
    }

    public byte[] delete(RemoveCmd cmd) {
        byte[] bs = HttpUtil.postJson(makeUrl(Route.DELETE), cmd, null);
        return bs;
    }

    public byte[] put(PutCmd cmd) {
        return HttpUtil.postJson(makeUrl(Route.PUT), cmd, null);
    }

    public State state() {
        //send request to remote server and return the response
        byte[] bs = HttpUtil.get(makeUrl(Route.STATE), null);
        return JsonUtil.fromJson(new String(bs), State.class);

    }

    String makeUrl(String path) {
        return "http://" + host + ":" + httpPort + "/" + path;
    }


    public byte[] get(String url) {
        //send request to remote server and return the response
        byte[] bs = HttpUtil.get(makeUrl(url), null);
        return bs;

    }


    public KeyDetail getKeysDetail(String fileName) {

        byte[] bs = new byte[0];
        try {
            bs = HttpUtil.get(makeUrl(Route.FILE_KEYS_DETAIL + "?" + URLEncoder.encode("file=" + fileName, "UTF-8")), null);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        KeyDetail keyDetails = JsonUtil.fromJson(new String(bs), KeyDetail.class);
        return keyDetails;

    }

    @Override
    public int rangeStart() {
        return slot.start;
    }

    @Override
    public int rangeEnd() {
        return slot.end;
    }

    @Override
    public HashRing.Slot getSlot() {
        return this.slot;
    }

    @Override
    public void setSlot(HashRing.Slot slot) {
        this.slot = slot;
    }

    @Override
    public int getHttpPort() {
        return httpPort;
    }

    @Override
    public boolean healthy() {
        //todo 检测 node 的健康信息
        return true;
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
