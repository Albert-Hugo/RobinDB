package com.ido.robin.sstable;

import java.util.List;

/**
 * @author Ido
 * @date 2020/12/24 14:24
 */
public class IndexFile {

    /**
     * 索引文件包含的segment 文件 的头部信息
     */
    private List<SegmentHeader> segmentHeaders;


    public List<SegmentHeader> getSegmentHeaders() {
        return segmentHeaders;
    }

    public IndexFile setSegmentHeaders(List<SegmentHeader> segmentHeaders) {
        this.segmentHeaders = segmentHeaders;
        return this;
    }
}
