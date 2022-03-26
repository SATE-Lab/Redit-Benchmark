package bean;

import java.io.Serializable;

public class MapTaskJoinArgs implements TaskArgs, Serializable {
    private int fileId;
    private int workerId;
    private Class<?>[] paramTypes = new Class[]{MapTaskJoinArgs.class, MapTaskJoinReply.class};

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

}
