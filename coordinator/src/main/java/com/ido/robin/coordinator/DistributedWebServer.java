package com.ido.robin.coordinator;

import com.ido.robin.common.HttpUtil;
import com.ido.robin.common.JsonUtil;
import com.ido.robin.server.constant.Route;
import com.ido.robin.server.controller.dto.GetCmd;
import com.ido.robin.server.controller.dto.GetKeysDetailCmd;
import com.ido.robin.server.controller.dto.KeyDetail;
import com.ido.robin.server.controller.dto.PutCmd;
import com.ido.robin.server.controller.dto.RemoveCmd;
import com.ido.robin.sstable.dto.State;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Objects;

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
    private boolean health = true;

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

    private String makeUrl(String path) {
        return "http://" + host + ":" + httpPort + "/" + path;
    }


    public byte[] get(GetCmd getCmd) {
        String url = Route.GET + String.format("?key=%s", getCmd.key);
        byte[] bs = HttpUtil.get(makeUrl(url), null);
        return bs;

    }


    public KeyDetail getKeysDetail(GetKeysDetailCmd cmd) {

        byte[] bs = new byte[0];
        String url = String.format("file=%s&page=%s&pageSize=%s&keyRangeStart=%s", cmd.file, cmd.page, cmd.pageSize, cmd.keyRangeStart);
        try {
            bs = HttpUtil.get(makeUrl(Route.FILE_KEYS_DETAIL + "?" + URLEncoder.encode(url, "UTF-8")), null);
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
        return this.health;
    }

    @Override
    public void setHealth(boolean health) {
        this.health = health;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedWebServer that = (DistributedWebServer) o;
        return port == that.port &&
                httpPort == that.httpPort &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, httpPort);
    }

    @Override
    public String toString() {
        return "DistributedWebServer{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", httpPort=" + httpPort +
                '}';
    }
}
