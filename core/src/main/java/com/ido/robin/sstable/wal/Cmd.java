package com.ido.robin.sstable.wal;

/**
 * @author Ido
 * @date 2021/1/6 9:31
 */
public enum Cmd {
    PUT(1), DELETE(2);

    private int val;

    Cmd(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }

    public static Cmd fromVal(int v) {
        for (Cmd c : Cmd.values()) {
            if (c.val == v) {
                return c;
            }
        }

        return null;
    }

}



