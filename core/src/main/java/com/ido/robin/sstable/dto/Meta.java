package com.ido.robin.sstable.dto;

import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/8 16:52
 */
@Data
public class Meta {
    String filename;
    /**
     * 开始的key
     */
    String keyStart;
    /**
     * 结束的key
     */
    String keyEnd;
    /***
     * 文件block List 的 数量
     */
    long blockListSize;
    /**
     * 文件总长度
     */
    long fileLen;





}
