package com.ido.robin.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * @author Ido
 * @date 2021/5/10 15:22
 */
public class CompressUtil {

    public static byte[] compress(byte[] source) throws IOException {
        ByteArrayOutputStream ous = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(ous);
        deflaterOutputStream.write(source);
        deflaterOutputStream.finish();
        deflaterOutputStream.flush();

        return ous.toByteArray();
    }


    public static byte[] decompress(byte[] source) throws IOException {
        InputStream is = new ByteArrayInputStream(source);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(is);
        byte[] decompressData = new byte[1024];
        int read = 0;
        StringBuilder sb = new StringBuilder();
        while ((read = inflaterInputStream.read(decompressData)) != -1) {

            sb.append(new String(Arrays.copyOf(decompressData, read), "UTF-8"));
        }
        return sb.toString().getBytes();
    }
}
