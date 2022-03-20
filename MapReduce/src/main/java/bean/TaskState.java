package bean;

public class TaskState {
    private int beginSecond;
    private int workerId;
    private int fileId;

    public TaskState(int beginSecond, int workerId, int fileId) {
        this.beginSecond = beginSecond;
        this.workerId = workerId;
        this.fileId = fileId;
    }

    public int getBeginSecond() {
        return beginSecond;
    }

    public void setBeginSecond(int beginSecond) {
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
