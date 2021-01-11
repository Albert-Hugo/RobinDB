package com.ido.robin.sstable;

import java.io.IOException;

/**
 * @author Ido
 * @date 2020/12/25 10:04
 */
public class PrintInfo {
    private static String path = "D:\\robin-data\\";
    public static void main(String[] args) throws IOException {
//        D:\robin-data\9230897633344b1b838eea54e195cdb5.seg
//        SSTable ssTable = new SSTable("dd73b3b677354230aeac66b8dad79827.seg");

//        ssTable.getSegmentFiles()

        SegmentFile segmentFile = new SegmentFile(path+"7bb7271399bb402cb253c04a11110f45.seg");
        System.out.println(segmentFile.getBlockList().size());
        for(Block s : segmentFile.getBlockList()){
            System.out.println(s.key);
        }
    }


}
