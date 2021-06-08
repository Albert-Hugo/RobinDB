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
    private String path;
    private long dataSize;
    private int fileCount;

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
