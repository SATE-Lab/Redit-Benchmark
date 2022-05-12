public class hw {

    public void getValue(int i) {
        // add by Mistletoe
        Thread.sleep(300);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        System.out.println(i);
    }

    public void setValue(int i) {
        // add by Mistletoe
        Thread.sleep(300);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        System.out.println(i + 1);
    }

    public void setValue1(int i) {
        // add by Mistletoe
        Thread.sleep(300);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        System.out.println(i + 1);
        System.out.println(i + 2);
    }

    public void setValue2(int i) {
        // add by Mistletoe
        Thread.sleep(300);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        System.out.println(i + 1);
        System.out.println(i + 2);
        System.out.println(i + 3);
    }

    public static void main(String[] args) {
        // add by Mistletoe
        Thread.sleep(300);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        // 解锁
        lock.unlock();
        System.out.println("hello world");
    }
}
