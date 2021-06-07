package com.ido.robin.server.controller.dto;

import com.ido.robin.sstable.KeyValue;
import lombok.Data;

import java.util.List;

/**
 * @author Ido
 * @date 2021/6/7 13:44
 */
@Data
public class KeyDetail {
    private List<KeyValue> keys;
    private int total;


}
