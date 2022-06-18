public class hw {

    public void getValue(int i) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        // add by Mistletoe
        Thread.sleep(300);
    }

    public void setValue(int i) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        // add by Mistletoe
        Thread.sleep(300);
    }

    public void setValue1(int i) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        // add by Mistletoe
        Thread.sleep(300);
    }

    public void setValue2(int i) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        // add by Mistletoe
        Thread.sleep(300);
    }

    public static void main(String[] args) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        // add by Mistletoe
        Thread.sleep(300);
    }
}
