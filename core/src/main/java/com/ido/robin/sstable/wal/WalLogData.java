package com.ido.robin.sstable.wal;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * write ahead log data
 *
 * @author Ido
 * @date 2021/1/6 9:25
 */
public class WalLogData {
    public final static int PUT = 1;
    public final static int DELETE = 2;
    private int cmd;
    private int keyLen;
    private byte[] key;
    private int valLen;
    private byte[] val;
    private long sequence;

    public WalLogData(Cmd cmd, byte[] key, byte[] val, long sequence) {
        this.cmd = cmd.getVal();
        this.key = key;
        this.val = val;
        this.valLen = val != null ? val.length : 0;
        this.keyLen = key.length;
        this.sequence = sequence;
    }

    private WalLogData() {
    }

    /**
     * 将 byte[] 转换成 block List
     *
     * @return
     */
    public static List<WalLogData> read(byte[] walDatas) {
        if (walDatas == null || walDatas.length == 0) {
            return new ArrayList<>();
        }
        ByteBuffer fileBf = ByteBuffer.wrap(walDatas);
        List<WalLogData> blocks = new ArrayList<>();
        while (true) {
            WalLogData d = fromBytes(fileBf);
            blocks.add(d);
            if (fileBf.remaining() < (4)) {
                break;
            }
        }


        return blocks;

    }

    public static WalLogData fromBytes(ByteBuffer bf) {
        WalLogData walLogData = new WalLogData();
        walLogData.cmd = bf.getInt();
        walLogData.keyLen = bf.getInt();
        walLogData.key = new byte[walLogData.keyLen];
        bf.get(walLogData.key);
        walLogData.valLen = bf.getInt();
        walLogData.val = new byte[walLogData.valLen];
        bf.get(walLogData.val);
        walLogData.sequence = bf.getLong();

        return walLogData;

    }

    public static WalLogData fromBytes(byte[] bs) {
        ByteBuffer bf = ByteBuffer.wrap(bs);
        return fromBytes(bf);

    }

    public static class WalLogDataComparetor implements Comparator<WalLogData> {

        @Override
        public int compare(WalLogData a1, WalLogData a2) {
            if(a1 == null && a2 == null){
                return 0;
            }
            if(a1 == null){
                return -1;
            }

            if(a2 == null){
                return 1;
            }
            if(a1.sequence - a2.sequence >0 ){
                return 1;
            }else if(a1.sequence - a2.sequence <0){
                return -1;
            }

            return 0;
        }
    }


    public byte[] toBytes() {
        ByteBuffer bf = ByteBuffer.allocate(4 + 4 + keyLen + 4 + valLen + 8);
        bf.putInt(this.cmd);
        bf.putInt(this.keyLen);
        bf.put(this.key);
        bf.putInt(this.valLen);
        if (this.val != null) {

            bf.put(this.val);
        }
        bf.putLong(sequence);
        return bf.array();
    }

    public Cmd getCmd() {
        return Cmd.fromVal(this.cmd);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getVal() {
        return val;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WalLogData that = (WalLogData) o;
        return cmd == that.cmd &&
                keyLen == that.keyLen &&
                valLen == that.valLen &&
                sequence == that.sequence &&
                Arrays.equals(key, that.key) &&
                Arrays.equals(val, that.val);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(cmd, keyLen, valLen, sequence);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(val);
        return result;
    }
}
