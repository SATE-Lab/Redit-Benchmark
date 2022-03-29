package mapreduce.bean;

import java.io.Serializable;

public class ReduceTaskReply implements TaskReply, Serializable {
    private int rIndex;
    private int nReduce;
    private int fileCount;

    public int getRIndex() {
        return rIndex;
    }

    public void setRIndex(int rIndex) {
        this.rIndex = rIndex;
    }

    public int getNReduce() {
        return nReduce;
    }

    public void setNReduce(int nReduce) {
        this.nReduce = nReduce;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public boolean isAllDone() {
        return allDone;
    }

    public void setAllDone(boolean allDone) {
        this.allDone = allDone;
    }

    private boolean allDone;
}
