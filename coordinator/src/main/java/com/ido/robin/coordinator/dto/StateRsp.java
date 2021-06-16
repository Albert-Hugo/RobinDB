package com.ido.robin.coordinator.dto;

import com.ido.robin.sstable.dto.State;
import lombok.Data;

/**
 * @author Ido
 * @date 2021/6/16 9:48
 */
@Data
public class StateRsp {
    State state;
    String host;
    int port;

}
