package com.ido.robin.server.metrics;

import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

/**
 * @author Ido
 * @date 2021/6/8 13:48
 */

public class MetricsConfig {

    private static HTTPServer server;


    public static void registerMetrics() {
        int port = Integer.getInteger("metrics.port", 1234);
        try {
            server = new HTTPServer(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
