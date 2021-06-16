package com.ido.robin.sstable.dto;

import lombok.Data;

import java.util.List;

/**
 * @author Ido
 * @date 2021/6/8 16:51
 */
@Data
public class State {
    List<Meta> metas;
    /**
     * 数据文件存储路径
     */
    private String path;
    /**
     * 总文件大小
     */
    private long dataSize;
    /**
     * 总文件数量
     */
    private int fileCount;
    /**
     * 总key 数量
     */
    private long keyCount;

    public List<Meta> getMetas() {
        return metas;
    }

    public String getPath() {
        return path;
    }

    public long getDataSize() {
        return dataSize;
    }

    public int getFileCount() {
        return fileCount;
    }

    @Override
    public String toString() {
        return "State{" +
                "metas=" + metas +
                ", path='" + path + '\'' +
                ", dataSize=" + dataSize +
                ", fileCount=" + fileCount +
                '}';
    }
}
