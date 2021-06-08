package com.ido.robin.sstable.dto;

import com.ido.robin.sstable.SegmentHeader;
import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/8 16:52
 */
@Data
public class Meta {
    SegmentHeader metadata;
    String filename;

    public Meta(SegmentHeader metadata, String filename) {
        this.metadata = metadata;
        this.filename = filename;
    }

    @Override
    public String toString() {
        return "Meta{" +
                "metadata=" + metadata +
                ", filename='" + filename + '\'' +
                '}';
    }


}
