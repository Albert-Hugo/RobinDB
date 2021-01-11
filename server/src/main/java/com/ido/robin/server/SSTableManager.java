package com.ido.robin.server;

import com.ido.robin.common.Config;
import com.ido.robin.sstable.SSTable;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Ido
 * @date 2019/1/18 14:16
 */
@Slf4j
public class SSTableManager {
    static SSTable ssTable;
    private static String DB_PATH;

    static {
        try {
            DB_PATH = Config.getInstance().getStringValue("db.path", System.getProperty("user.dir"));
            ssTable = new SSTable(DB_PATH);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    public static SSTable getInstance() {
        return ssTable;
    }

    public static String getDbPath() {
        return DB_PATH;
    }
}
