package bean;

public class ReduceTaskJoinArgs implements TaskArgs{
    private int workId;
    private int rIndex;

    public int getWorkId() {
        return workId;
    }

    public void setWorkId(int workId) {
        this.workId = workId;
    }

    public int getRIndex() {
        return rIndex;
    }

    public void setRIndex(int rIndex) {
        this.rIndex = rIndex;
    }
}
