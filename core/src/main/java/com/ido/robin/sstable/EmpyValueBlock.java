package com.ido.robin.sstable;

/**
 * @author Ido
 * @date 2019/1/30 15:19
 */
public class EmpyValueBlock extends Block {
    @Override
    public byte[] getVal() {
        return null;
    }
}
