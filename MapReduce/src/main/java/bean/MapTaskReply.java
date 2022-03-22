package bean;

import java.io.File;

public class MapTaskReply {
    // worker passes this to the os package
    private File file;

    // marks a unique file for mapping
    // gives -1 for no more fileId
    private int fileId;

    // for reduce tasks
    private int nReduce;

    // assign worker id as this reply is the first sent to workers
    private int workId;

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

    public int getnReduce() {
        return nReduce;
    }

    public void setnReduce(int nReduce) {
        this.nReduce = nReduce;
    }

    public int getWorkId() {
        return workId;
    }

    public void setWorkId(int workId) {
        this.workId = workId;
    }

    public boolean isAllDone() {
        return allDone;
    }

    public void setAllDone(boolean allDone) {
        this.allDone = allDone;
    }
}
