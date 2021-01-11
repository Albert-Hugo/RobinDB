package com.ido.robin.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Ido
 * @date 2019/2/16 11:10
 */
@Slf4j
public class HttpUtil {
    private static CloseableHttpClient client;
    private final static Gson gson = new GsonBuilder().create();

    static {
        client = httpClient();
    }

    public static CloseableHttpClient httpClient() {
        // The final survival time also depends on the server's keep-alive settings, idle time, and intermittent validation.
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager(30, TimeUnit.SECONDS);
        pool.setMaxTotal(2000);
        // At present, there is only one route, which is equal to the maximum by default. It is set according to the traffic concurrency.
        pool.setDefaultMaxPerRoute(2000);
        //Check the inactive connection to avoid the failure caused by the active closing of the connection after the server restarts or the keep alive expires. It may be common for the microservice scenario, but it is limited by the HTTP design concept, which is also completely reliable. Use the re try / re execute mechanism to make up for this. Considering that many niginx may be configured to keep alive for 5 seconds.
        pool.setValidateAfterInactivity(5 * 1000);
        return HttpClients.custom()
                .setConnectionManager(pool)
                // Recovery occurs when the connection is idle for 10 seconds. This will start independent thread detection, so the destroy method must be declared to close the independent thread.
                .evictIdleConnections(10, TimeUnit.SECONDS)
                //Establish connection time, get connection time from connection pool, and data transfer time
                .setDefaultRequestConfig(RequestConfig.custom()
                        // HTTP connection establishment timeout
                        .setConnectTimeout(1000)
                        // Getting connection timeout from connection pool
                        .setConnectionRequestTimeout(3000)
                        // socket timeout
                        .setSocketTimeout(10000)
                        .build())
                //Custom retry mechanism
                .setRetryHandler((exception, executionCount, context) -> {
                    // At present, only one retry is allowed.
                    if (executionCount > 1) {
                        return false;
                    }
                    // If the server actively closes the connection, the data is not accepted by the server and can be retried.
                    if (exception instanceof NoHttpResponseException) {
                        return true;
                    }
                    //Do not retry SSL handshake exception
                    if (exception instanceof SSLHandshakeException) {
                        return false;
                    }
                    // timeout
                    if (exception instanceof InterruptedIOException) {
                        return false;
                    }
                    //Target server unreachable
                    if (exception instanceof UnknownHostException) {
                        return false;
                    }
                    // Exceptional handshake in SSL
                    if (exception instanceof SSLException) {
                        return false;
                    }
                    HttpClientContext clientContext = HttpClientContext.adapt(context);
                    HttpRequest request = clientContext.getRequest();
                    String get = "GET";
                    // The GET method is idempotent and can be retried
                    if (request.getRequestLine().getMethod().equalsIgnoreCase(get)) {
                        return true;
                    }
                    return false;
                })
                //The default connectionkeepalivestrategy is dynamically calculated based on keep alive
                .build();
    }

    public static byte[] get(String url, Map<String, String> headers) {
        HttpGet get = new HttpGet(url);
        try {
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    get.setHeader(entry.getKey(), entry.getValue());
                }
            }
            CloseableHttpResponse response = client.execute(get);
            InputStream in = response.getEntity().getContent();
            byte[] data = new byte[in.available()];
            in.read(data);
            response.close();
            return data;
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        return null;
    }

    public static byte[] delete(HttpDelete httpPost, Map<String, String> headers) {
        try {
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpPost.setHeader(entry.getKey(), entry.getValue());
                }
            }
            CloseableHttpResponse response = client.execute(httpPost);
            InputStream in = response.getEntity().getContent();
            byte[] d = new byte[in.available()];
            in.read(d);
            response.close();
            return d;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return null;
    }


    public static byte[] postForm(HttpPost httpPost, byte[] data, Map<String, String> headers) {
        try {
            httpPost.setEntity(new ByteArrayEntity(data));
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpPost.setHeader(entry.getKey(), entry.getValue());
                }
            }
            CloseableHttpResponse response = client.execute(httpPost);
            InputStream in = response.getEntity().getContent();
            byte[] d = new byte[in.available()];
            in.read(d);
            response.close();
            return d;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return null;
    }

    public static byte[] postForm(String url, byte[] data, Map<String, String> headers) {
        HttpPost httpPost = new HttpPost(url);
        return postForm(httpPost, data, headers);
    }

    public static byte[] postJson(String url, Object vo, Map<String, String> headers) {
        HttpPost httpPost = new HttpPost(url);
        StringEntity entity = null;
        try {
            entity = new StringEntity(gson.toJson(vo));
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage());
        }
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try {
            httpPost.setEntity(entity);
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpPost.setHeader(entry.getKey(), entry.getValue());
                }
            }
            CloseableHttpResponse response = client.execute(httpPost);
            InputStream in = response.getEntity().getContent();
            byte[] d = new byte[in.available()];
            in.read(d);
            response.close();
            return d;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return null;
    }
}
