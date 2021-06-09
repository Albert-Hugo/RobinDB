package com.ido.robin.coordinator;

import com.ido.robin.server.Server;

/**
 * @author Ido
 * @date 2019/2/15 10:25
 */
public interface DistributedServer extends Server, coordinable {
    /**
     * 获取哈希环槽位
     *
     * @return
     */
    HashRing.Slot getSlot();

    void setSlot(HashRing.Slot slot);

    /**
     * 是否包含指定的slot 范围
     *
     * @param slot
     * @return
     */
    default boolean includeSlot(HashRing.Slot slot) {
        return this.rangeStart() <= slot.start && this.rangeEnd() >= slot.end;
    }

    int getHttpPort();

    boolean healthy();

    void setHealth(boolean health);
}
