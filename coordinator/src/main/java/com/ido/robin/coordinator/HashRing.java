package com.ido.robin.coordinator;

import com.ido.robin.server.Server;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 哈希环
 *
 * @author Ido
 * @date 2019/2/15 10:28
 */
public class HashRing {

    LinkedList<Node> nodes;

    public static int getMaxRange() {
        return Integer.MAX_VALUE;
    }

    public HashRing(int slotNum) {
        this.nodes = new LinkedList<>();
        int addor = getMaxRange() / slotNum;
        for (int i = 0; i < slotNum; i++) {
            if (i == slotNum - 1) {
                //防止整除之后没有包含最大值
                this.nodes.add(new Node(new Slot(i * addor, getMaxRange())));
            } else {
                this.nodes.add(new Node(new Slot(i * addor, (i + 1) * addor)));
            }
        }
    }

    /**
     * 移除指定slot
     *
     * @param slot 移除的slot
     * @return slot 被合并的新slot
     */
    public Slot removeSlot(Slot slot) {
        this.nodes.remove(new Node(slot));
        Node nextNode;
        if (slot.end == getMaxRange()) {
            //最后一个节点被移除的情况下
            nextNode = this.nodes.getFirst();
        } else {
            nextNode = this.nodes.stream().filter(n -> n.getSlot().start == slot.end).findAny().orElse(null);
        }
        nextNode.slot = new Slot(slot.start, nextNode.slot.end);

        return nextNode.slot;

    }

    /**
     * 获取没使用的槽位
     *
     * @return
     */
    public Collection<Slot> emptySlots() {
        return this.nodes.stream().filter(n -> !n.used).map(n -> n.slot).collect(Collectors.toList());
    }

    private static class NodeComparator implements Comparator<Node> {
        @Override
        public int compare(Node o1, Node o2) {
            return (o1.slot.end - o1.slot.start) - (o2.slot.end - o2.slot.start);
        }
    }

    static class Node {
        private Slot slot;
        private boolean used;

        public Node(Slot slot) {
            this.slot = slot;
            this.used = false;
        }

        public Slot getSlot() {
            return slot;
        }

        public boolean isUsed() {
            return used;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return slot.equals(node.slot);
        }

        @Override
        public int hashCode() {
            return Objects.hash(slot);
        }
    }

    /**
     * 在哈希环上添加一个槽位
     * 将原来最大的槽位分成2个
     *
     * @return 原来最大的槽位
     */
    Slot addSlot() {
        this.nodes.sort(new NodeComparator());
        Node toSplit = this.nodes.get(0);
        int mid = (toSplit.slot.end - toSplit.slot.start) / 2;
        Slot s1 = new Slot(toSplit.slot.start, mid);
        Slot s2 = new Slot(mid, toSplit.slot.end);
        Node removedNode = this.nodes.removeFirst();
        this.nodes.add(new Node(s1));
        this.nodes.add(new Node(s2));

        return removedNode.slot;

    }

    /**
     * 分配环上的节点
     *
     * @param server
     * @return
     */
    public Slot locateSlot(Server server) {
        for (Node n : nodes) {
            if (!n.used) {
                n.used = true;
                return n.slot;
            }
        }
        throw new IllegalStateException("No slot can be allocated");
    }

    List<Slot> getNodes() {
        return nodes.stream().map(n -> n.slot).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "HashRing{" +
                ", nodes=" + nodes +
                '}';
    }

    static class Slot {
        int start;
        int end;

        public Slot(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "Slot{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Slot slot = (Slot) o;
            return start == slot.start &&
                    end == slot.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }
    }

    public static void main(String[] args) {
        System.out.println(new HashRing(3));
    }


}
