package com.ido.robin.client;

import com.ido.robin.client.netty.Connector;
import com.ido.robin.rpc.proto.RemoteCmd;



/**
 * @author Ido
 * @date 2019/1/24 10:18
 */
public class RobinClient {

    private Connector connector;

    public RobinClient(String host, int port) {
        this.connector = new Connector(host, port);
        this.connector.connect();
    }

    public void copyData(String fileName, byte[] data) {
        this.connector.copy(fileName, data);
    }

    public void sendRemoteCopyRequest(String host, int port, int start, int end, RemoteCmd.RemoteCopyRequest.CopyType type) {
        this.connector.sendRemoteCopyRequest(host, port, start, end, type);
    }

    public void put(String k, String v) {
        connector.put(k, v);

    }

    public void delete(String k) {
        connector.delete(k);

    }

    public String get(String k) {
        return connector.get(k);

    }

    public void shutdown() {
        connector.close();
    }


}
