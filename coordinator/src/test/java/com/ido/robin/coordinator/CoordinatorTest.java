package com.ido.robin.coordinator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ido
 * @date 2019/2/20 19:35
 */
public class CoordinatorTest {

    @Test
    public void testAddNode() {
        List list = new ArrayList<>();
        list.add(new DistributedWebServer("test", "localhost", 8688, 8888));
        list.add(new DistributedWebServer("test", "localhost", 18688, 18888));
        Coordinator coordinator = new Coordinator(list);
        DistributedWebServer newNode = new DistributedWebServer("test", "localhost", 18689, 18889);
        coordinator.addNode(newNode);
    }


    @Test
    public void testRemoveNode() {
        List list = new ArrayList<>();
        list.add(new DistributedWebServer("test", "localhost", 8688, 8888));
        list.add(new DistributedWebServer("test", "localhost", 18688, 18888));
        list.add(new DistributedWebServer("test", "localhost", 18689, 18889));
        Coordinator coordinator = new Coordinator(list);
        DistributedWebServer newNode = new DistributedWebServer("test", "localhost", 18689, 18888);
        coordinator.removeNode("localhost", 18689);
    }
}
