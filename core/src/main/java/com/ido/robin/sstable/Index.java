package com.ido.robin.sstable;

/**
 * @author Ido
 * @date 2019/1/1 13:23
 */
public interface Index extends FileManager.SegmentFileChangeListener {

    /**
     * search block from the all target files
     * <p>
     * if not found , will return @{null}
     *
     * @param key the target block key
     * @return the target block found
     */
    Block search(String key);

}
