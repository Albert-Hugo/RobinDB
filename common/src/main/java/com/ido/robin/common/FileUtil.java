package com.ido.robin.common;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Ido
 * @date 2019/1/23 15:43
 */
public class FileUtil {

    public static void closeQuietly(Closeable is) {
        try {
            if (is != null) {
                is.close();
            }
        } catch (IOException e) {
        }
    }
}
