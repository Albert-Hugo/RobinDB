package com.ido.robin.client;

import com.ido.robin.client.netty.Connector;
import com.ido.robin.rpc.proto.RemoteCmd;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Ido
 * @date 2019/2/20 14:17
 */
public class ConnectorTest {

    @Test
    public void testCopy() throws IOException {
        Connector connector = new Connector("localhost", 8688);
        connector.connect();
        FileInputStream fs = new FileInputStream("D:\\software\\protoc-3.13.0-win64\\bin\\com\\ido\\robin\\rpc\\proto\\RemoteCmd.java");
        byte[] bs = new byte[fs.available()];
        fs.read(bs);
        connector.copy("Remote.java", bs);
        connector.close();
        ;
    }

    @Test
    public void testSet() throws IOException {
        Connector connector = new Connector("localhost", 8688);
        connector.connect();
        connector.put("Remote.java", "fsf");
        System.out.println(connector.get("Remote.java"));
        ;
        connector.close();
        ;
    }

    @Test
    public void testSendRemoteRequest() throws IOException {
        Connector connector = new Connector("localhost", 8688);
        connector.connect();
        connector.sendRemoteCopyRequest("localhost", 8688, 0, 10000, RemoteCmd.RemoteCopyRequest.CopyType.ADD);
        connector.close();
        ;
    }
}
