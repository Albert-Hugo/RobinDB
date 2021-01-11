package com.ido.robin.sstable;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The segment header
 * contains meta data about Segment file
 *
 * @author Ido
 * @date 2019/1/10 9:40
 */
public class SegmentHeader {
    /**
     * 开始的key
     */
    String keyStart;
    /**
     * 结束的key
     */
    String keyEnd;
    /**
     * 头长度
     */
    long headerLength;
    /**
     * 开始key 的长度
     */
    long startLen;
    /**
     * 结束key 的长度
     */
    long endLen;
    /***
     * 文件block List 的 数量
     */
    long blockListSize;
    /**
     * 文件总长度
     */
    long fileLen;

    long segmentFileNameLen;
    String segmentFileName;

    public boolean isKeyBetweenFile(String key) {
        if(this.fileLen == 0) return false;
        return key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0;
    }

    /**
     * 获取segment file 的头部信息
     *
     * @param segmentFile
     * @return
     * @throws IOException
     */
    public static SegmentHeader getHeaderInfo(String segmentFile) throws IOException {
        try (FileInputStream fs = new FileInputStream(segmentFile)) {

            byte[] headerLenData = new byte[8];
            fs.read(headerLenData, 0, 8);
            ByteBuffer headerBytes = ByteBuffer.wrap(headerLenData);
            int headerLen = (int) headerBytes.getLong();
            if (headerLen == 0) {
                return new SegmentHeader();
            }

            byte[] headerLeftData = new byte[headerLen - 8];
            fs.read(headerLeftData);
            byte[] headerAllData = new byte[headerLen];
            System.arraycopy(headerLenData, 0, headerAllData, 0, headerLenData.length);
            System.arraycopy(headerLeftData, 0, headerAllData, headerLenData.length, headerLeftData.length);
            fs.close();
            return fromBytes(headerAllData);
        }
    }

    static SegmentHeader fromBytes(byte[] bs) {
        SegmentHeader header = new SegmentHeader();
        ByteBuffer fileBf = ByteBuffer.wrap(bs);
        header.headerLength = fileBf.getLong();
        header.fileLen = fileBf.getLong();
        header.blockListSize = fileBf.getLong();
        header.startLen = fileBf.getLong();
        header.endLen = fileBf.getLong();
        byte[] keyS = new byte[(int) header.startLen];
        byte[] keyE = new byte[(int) header.endLen];

        fileBf.get(keyS);
        fileBf.get(keyE);
        header.keyStart = new String(keyS);
        header.keyEnd = new String(keyE);

        header.segmentFileNameLen = fileBf.getLong();
        byte[] fileName = new byte[(int) header.segmentFileNameLen];
        fileBf.get(fileName);
        header.segmentFileName = new String(fileName);

        return header;
    }

    public byte[] toBytes() {
        byte[] s = this.keyStart.getBytes();
        byte[] e = this.keyEnd.getBytes();
        startLen = s.length;
        endLen = e.length;
        headerLength = this.segmentFileName.getBytes().length + s.length + e.length + 8 + 8 + 8 + 8 + 8 + 8;
        ByteBuffer bf = ByteBuffer.allocate((int) headerLength);
        bf.putLong(headerLength);
        bf.putLong(this.fileLen);
        bf.putLong(this.blockListSize);
        bf.putLong(startLen);
        bf.putLong(endLen);
        bf.put(s);
        bf.put(e);

        bf.putLong(this.segmentFileName.getBytes().length);
        bf.put(this.segmentFileName.getBytes());


        return bf.array();
    }

    @Override
    public String toString() {
        return "SegmentHeader{" +
                "keyStart='" + keyStart + '\'' +
                ", keyEnd='" + keyEnd + '\'' +
                ", headerLength=" + headerLength +
                ", startLen=" + startLen +
                ", endLen=" + endLen +
                ", blockListSize=" + blockListSize +
                ", fileLen=" + fileLen +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentHeader header = (SegmentHeader) o;
        return headerLength == header.headerLength &&
                startLen == header.startLen &&
                endLen == header.endLen &&
                blockListSize == header.blockListSize &&
                fileLen == header.fileLen &&
                segmentFileNameLen == header.segmentFileNameLen &&
                Objects.equals(keyStart, header.keyStart) &&
                Objects.equals(keyEnd, header.keyEnd) &&
                Objects.equals(segmentFileName, header.segmentFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyStart, keyEnd, headerLength, startLen, endLen, blockListSize, fileLen);
    }


}
