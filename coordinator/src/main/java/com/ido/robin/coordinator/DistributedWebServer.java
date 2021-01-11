package com.ido.robin.coordinator;

import com.ido.robin.common.HttpUtil;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;

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

    public byte[] delete(String url) {
        //send request to remote server and return the response
        //send request to remote server and return the response
        HttpDelete post = new HttpDelete(makeUrl(url));
        byte[] bs = HttpUtil.delete(post, null);
        return bs;
    }

    public byte[] put(byte[] data) {
        //send request to remote server and return the response
        HttpPost post = new HttpPost(makeUrl("put"));
        post.setEntity(new ByteArrayEntity(data));
        HttpUtil.postForm(post, data, null);
        byte[] bs = HttpUtil.postForm(post, data, null);
        return bs;

    }

    String makeUrl(String path) {
        return "http://" + host + ":" + httpPort + "/" + path;
    }


    public byte[] get(String url) {
        //send request to remote server and return the response
        byte[] bs = HttpUtil.get(makeUrl(url), null);
        return bs;

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
