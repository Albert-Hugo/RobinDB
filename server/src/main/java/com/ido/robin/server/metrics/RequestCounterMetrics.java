package com.ido.robin.server.metrics;

import io.prometheus.client.Counter;

/**
 * @author Ido
 * @date 2021/6/8 14:15
 */
public class RequestCounterMetrics {

    static final Counter requests = Counter.build()
            .name("robin_requests_total")
            .labelNames("path")
            .help("Total requests.").register();


    public static void inc(String methodName) {
        requests.labels(methodName).inc();

    }


}
