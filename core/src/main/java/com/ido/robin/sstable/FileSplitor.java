package com.ido.robin.sstable;

import java.util.List;

/**
 * 文件拆分管理，当segment file 过大时，自动拆分成小的文件
 *
 * @author Ido
 * @date 2019/1/11 14:23
 */
public interface FileSplitor {
    /**
     * 获取指定目录下需要被 拆分的文件
     *
     * @param path
     * @return
     */
    List<SegmentFile> listToBeSplittedSgFile(String path);

    /**
     * 拆分指定的文件
     *
     * @param segmentFile
     * @return 拆分后的多个 文件
     */
    List<String> split(SegmentFile segmentFile);


}
