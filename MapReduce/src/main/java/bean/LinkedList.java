package bean;

public class LinkedList {
    private Node head;
    private int size;

    public int getSize() {
        return size;
    }

    public LinkedList(int size, Node head) {
        this.head = head;
        this.size = size;
    }

    //定义节点内部静态类
    private static class Node {
        Object data;
        Node prev;
        Node next;

        Node(Object data, Node prev, Node next) {
            this.data = data;
            this.next = next;
            this.prev = prev;
        }
    }

    /**
     * Insert a new node before the current node.
     * @param currentNode
     * @param data
     */
    public void pushFront(Node currentNode, Object data) {
        Node newNode = new Node(data, null, null);
        if (size == 0) {
            this.head = newNode;
        }else {
            Node prevNode = currentNode.prev;
            newNode.next = currentNode;
            currentNode.prev = newNode;
            newNode.prev = prevNode;
            prevNode.next = newNode;
        }
        this.size++;
    }

    /**
     * Insert a new node after the current node.
     * @param currentNode
     * @param data
     */
    public void pushBack(Node currentNode, Object data) {
        Node newNode = new Node(data, null, null);
        if (size == 0) {
            this.head = newNode;
        }else {
            Node nextNode = currentNode.next;
            newNode.prev = currentNode;
            currentNode.next = newNode;
            newNode.next = nextNode;
            nextNode.prev = newNode;
        }
        this.size++;
    }

    public void peekFront(){
        // TODO
    }

    public void peekBack(){
        // TODO
    }

    public void popFront(){
        // TODO
    }

    public void popBack(){
        // TODO
    }

}