package com.ido.robin.sstable.wal;

import com.ido.robin.common.IdWorker;

/**
 * @author Ido
 * @date 2021/1/6 14:58
 */
public class SequenceManager {
    private IdWorker idWorker = new IdWorker(1, 1, 0);


    public long next() {
        return idWorker.nextId();
    }
}
