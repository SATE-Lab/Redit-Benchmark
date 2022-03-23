package bean;

public class MapTaskArgs implements TaskArgs{
    // -1 if does not have one
    private int workerId;

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }
}
