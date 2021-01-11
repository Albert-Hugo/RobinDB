package com.ido.robin.coordinator;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2019/2/15 13:39
 */
public class HashRingTest {

    @Test
    public void testHashRingSlot() {
        HashRing ring = new HashRing(3);

        Assert.assertEquals(3, ring.getNodes().size());
        Assert.assertEquals(0, ring.getNodes().get(0).start);
        Assert.assertEquals(ring.getNodes().get(0).end, ring.getNodes().get(1).start);
        Assert.assertEquals(ring.getNodes().get(1).end, ring.getNodes().get(2).start);
    }

    @Test
    public void testRemoveSlot() {
        HashRing ring = new HashRing(3);
        ring.locateSlot(null);
        ring.locateSlot(null);
        ring.locateSlot(null);
        HashRing.Slot slot = ring.getNodes().get(0);
        HashRing.Slot mergedSlot = ring.removeSlot(slot);
        Assert.assertNotNull(mergedSlot);
        Assert.assertEquals(2, ring.nodes.size());

    }

    @Test
    public void testAddSlot() {
        HashRing ring = new HashRing(3);
        ring.locateSlot(null);
        ring.locateSlot(null);
        ring.locateSlot(null);
        HashRing.Slot removed = ring.addSlot();
        Assert.assertNotNull(removed);
        Assert.assertEquals(4, ring.nodes.size());

        Assert.assertEquals(2, ring.nodes.stream().filter(node -> node.isUsed()).collect(Collectors.toList()).size());
    }


    @Test
    public void testLocateSlot() {
        HashRing ring = new HashRing(3);
        HashRing.Slot slot = ring.locateSlot(null);
        HashRing.Slot slot2 = ring.locateSlot(null);
        HashRing.Slot slot3 = ring.locateSlot(null);

        Assert.assertNotEquals(slot, slot2);
        Assert.assertNotEquals(slot2, slot3);


    }
}
