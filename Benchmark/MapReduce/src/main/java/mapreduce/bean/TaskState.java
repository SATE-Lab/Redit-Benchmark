package mapreduce.bean;

public class TaskState {
    private long beginSecond;
    private int workerId;
    private int fileId;

    public long getBeginSecond() {
        return beginSecond;
    }

    public void setBeginSecond(long beginSecond) {
        this.beginSecond = beginSecond;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }
}
