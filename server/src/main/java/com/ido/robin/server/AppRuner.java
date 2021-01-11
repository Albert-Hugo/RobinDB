package com.ido.robin.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Ido
 * @date 2019/1/18 14:12
 */
public class AppRuner {


    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        int rp = Integer.getInteger("remote.port", 8688);
        executorService.execute(() -> {
            try {
                new ServerStarter().start(rp);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        int wp = Integer.getInteger("web.port", 8888);

        executorService.execute(() -> {
            new WebFacadeServer().start(wp);
        });
    }
}
