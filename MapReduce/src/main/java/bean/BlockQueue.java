package bean;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockQueue {
    private LinkedList list;
    private Lock lock;
    private Condition condition;

    public BlockQueue() {
        this.list = new LinkedList();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    /**
     * Insert into the head of the queue.
     * @param data
     */
    public void PutFront(Object data){
        this.lock.lock();
        this.list.pushFront(data);
        this.lock.unlock();
        this.condition.signalAll();
    }

    /**
     * Insert at the end of the queue.
     * @param data
     */
    public void PutBack (Object data){
        this.lock.lock();
        this.list.pushBack(data);
        this.lock.unlock();
        this.condition.signalAll();
    }

    /**
     * Queue head element performs dequeue.
     * @return head element's data
     * @throws Exception
     */
    public Object GetFront () throws Exception {
        this.lock.lock();
        while (this.list.getSize() == 0){
            this.condition.await();
        }
        Object data = this.list.popFront();
        this.lock.unlock();
        return data;
    }

    /**
     * Queue end element performs dequeue.
     * @return end element's data
     * @throws Exception
     */
    public Object GetBack () throws Exception {
        this.lock.lock();
        while (this.list.getSize() == 0){
            this.condition.await();
        }
        Object data = this.list.popBack();
        this.lock.unlock();
        return data;
    }

    /**
     * Queue head element performs dequeue without await 'condition'.
     * @return head element's data
     * @throws Exception
     */
    public Object PopFront() {
        this.lock.lock();
        Object data = this.list.popFront();
        this.lock.unlock();
        return data;
    }

    /**
     * Queue end element performs dequeue without await 'condition'.
     * @return end element's data
     * @throws Exception
     */
    public Object PopBack() {
        this.lock.lock();
        Object data = this.list.popBack();
        this.lock.unlock();
        return data;
    }

    /**
     * Query the data of the head element in the queue.
     * @return head element's data
     * @throws Exception
     */
    public Object PeekFront() throws Exception {
        this.lock.lock();
        while (this.list.getSize() == 0){
            this.condition.await();
        }
        Object data = this.list.peekFront();
        this.lock.unlock();
        return data;
    }

    /**
     * Query the data of the end element in the queue.
     * @return end element's data
     * @throws Exception
     */
    public Object PeekBack() throws Exception {
        this.lock.lock();
        while (this.list.getSize() == 0){
            this.condition.await();
        }
        Object data = this.list.peekBack();
        this.lock.unlock();
        return data;
    }

    /**
     * Get the length of the queue.
     * @return queue size
     */
    public int getSize(){
        this.lock.lock();
        int size = this.list.getSize();
        this.lock.unlock();
        return size;
    }

}



