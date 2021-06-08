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
        try {
            server = new HTTPServer(1234);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
