package com.ido.robin.coordinator.dto;

import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/8 17:02
 */
@Data
public class NodeInfo {
    public String host;
    public int port;
    public boolean healthy;

}
