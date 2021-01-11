package com.ido.robin.sstable.wal;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Ido
 * @date 2021/1/6 10:22
 */
public class WalDataFile implements Closeable {
    public static final String WAL_FILE_PREFIX = ".wal";
    private List<WalLogData> logDataList = new ArrayList<>();
    private String fileName;
    private FileOutputStream fos;


    public static List<WalLogData> getWalLogDataFromFile(File file) throws IOException {
        FileInputStream fos = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fos);

        byte[] bf = new byte[fos.available()];
        bis.read(bf);
        List<WalLogData> logData = WalLogData.read(bf);
        bis.close();
        fos.close();
        return logData;
    }

    public static List<WalLogData> getWalLogDataFromFile(String fileName) throws IOException {
        return getWalLogDataFromFile(new File(fileName));
    }


    public WalDataFile(String fileName) throws FileNotFoundException {
        this(new File(fileName));
    }

    public WalDataFile(File file) throws FileNotFoundException {
        this.fileName = file.getName();
        fos = new FileOutputStream(file, true);
    }

    public void append(WalLogData data) throws IOException {
        logDataList.add(data);
        fos.write(data.toBytes());
        fos.flush();
    }

    public void batchAppend(WalLogData ...datas) throws IOException {
        for(WalLogData d : datas){
            append(d);
        }
    }
    public void batchAppend(List<WalLogData> datas) throws IOException {
        for(WalLogData d : datas){
            append(d);
        }
    }



    public String getFileName() {
        return fileName;
    }

    @Override
    public void close() throws IOException {
        fos.close();
    }

    /**
     * 根据文件名的序列号大小排序，最大的号为最新的文件
     */
    public static class FileNameComparator implements Comparator<WalDataFile> {

        @Override
        public int compare(WalDataFile o1, WalDataFile o2) {
            return - (o1.fileName.compareTo(o2.fileName));
        }
    }
}
