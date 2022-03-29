package mapreduce.bean;

import java.io.File;
import java.io.Serializable;

public class MapTaskReply implements TaskReply, Serializable {
    // worker passes this to the os package
    private File file;

    // marks a unique file for mapping
    // gives -1 for no more fileId
    private int fileId;

    // for reduce tasks
    private int nReduce;

    // assign worker id as this reply is the first sent to workers
    private int workerId;

    // whether this kind of tasks are all done
    // if not, and fileId is -1, the worker waits
    private boolean allDone;

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getNReduce() {
        return nReduce;
    }

    public void setNReduce(int nReduce) {
        this.nReduce = nReduce;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public boolean isAllDone() {
        return allDone;
    }

    public void setAllDone(boolean allDone) {
        this.allDone = allDone;
    }
}
