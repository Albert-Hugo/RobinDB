package com.ido.robin.server.controller.dto;

import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/8 15:57
 */
@Data
public class PutCmd {
    public String key;
    public String val;

    public PutCmd(String key, String val) {
        this.key = key;
        this.val = val;
    }
}
