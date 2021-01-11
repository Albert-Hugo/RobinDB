package com.ido.robin.sstable;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * @author Ido
 * @date 2019/8/31 17:29
 */
public class TestByteBuf {
    public static void main(String[] args) throws UnsupportedEncodingException {
        int len = "dsaa".getBytes().length;
        ByteBuffer bf = ByteBuffer.allocate(len);
        byte[] origin = "dsaa".getBytes();
        bf.put(origin);
        byte[] bs = new byte[len];
        bf.rewind();
        bf.get(origin, 0, 1);
        System.out.println(new String(origin));
//        System.out.println(new String(bs, Charset.defaultCharset()));
    }

//    @Test
    public void test() throws IOException {
        FileInputStream file = new FileInputStream("test.seg");
        byte[] data = new byte[file.available()];


        file.read(data);

        System.out.println(new String(data));
    }
}
