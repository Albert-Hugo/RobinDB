package com.ido.robin.server.controller.dto;

import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/8 15:56
 */
@Data
public class GetKeysDetailCmd {
    public String file;
    public String pageSize = "10";
    public String page = "1";
}
