package com.ido.robin.common.exception;

/**
 * @author Ido
 * @date 2021/6/10 11:12
 */
public class BizException extends RuntimeException {
    public BizException(String message) {
        super(message);
    }
}
