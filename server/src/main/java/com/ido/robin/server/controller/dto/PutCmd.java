package com.ido.robin.server.controller.dto;

import lombok.Data;

import static com.ido.robin.common.Constant.EXPIRED_TIME_PERMANENT;

/**
 * @author Ido
 * @date 2021/6/8 15:57
 */
@Data
public class PutCmd {
    public String key;
    public String val;
    public long expiredTime = EXPIRED_TIME_PERMANENT;

}
