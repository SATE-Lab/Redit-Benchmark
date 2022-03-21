package bean;

// 循环双链表
public class LinkedList {
    private Node head;
    private int size;

    public LinkedList() {
        this.head = new Node(null, null, null);
        head.next = head;
        head.prev = head;
        this.size = 0;
    }

    public int getSize() { return size; }

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
     * @param data
     */
    public void pushFront(Object data) {
        Node newNode = new Node(data, null, null);
        Node nextNode = this.head.next;
        newNode.prev = this.head;
        this.head.next = newNode;
        newNode.next = nextNode;
        nextNode.prev = newNode;
        this.size++;
    }

    /**
     * Insert a new node after the current node.
     * @param data
     */
    public void pushBack( Object data) {
        Node newNode = new Node(data, null, null);
        Node prevNode = this.head.prev;
        newNode.next = this.head;
        this.head.prev = newNode;
        newNode.prev = prevNode;
        prevNode.next = newNode;
        this.size++;
    }

    /**
     * Query the data of the first node of the linked list.
     * @return first node's data.
     * @throws Exception
     */
    public Object peekFront() throws Exception {
        if (this.size == 0){
            throw new Exception("peeking empty list");
        }
        return this.head.next.data;
    }

    /**
     * Query the data of the last node of the linked list.
     * @return last node's data.
     * @throws Exception
     */
    public Object peekBack() throws Exception {
        if (this.size == 0){
            throw new Exception("peeking empty list");
        }
        return this.head.prev.data;
    }

    /**
     * Pop the first node in the linked list.
     * @return first node's data.
     * @throws Exception
     */
    public Object popFront() throws Exception {
        if (this.size == 0){
            throw new Exception("popping empty list");
        }
        Object data = this.head.next.data;
        Node nextNode = head.next.next;
        head.next = nextNode;
        nextNode.prev = head;
        this.size--;
        return data;
    }

    /**
     * Pop the last node in the linked list.
     * @return last node's data.
     * @throws Exception
     */
    public Object popBack() throws Exception {
        if (this.size == 0){
            throw new Exception("popping empty list");
        }
        Object data = this.head.prev.data;
        Node prevNode = head.prev.prev;
        head.prev = prevNode;
        prevNode.next = head;
        this.size--;
        return data;
    }

}