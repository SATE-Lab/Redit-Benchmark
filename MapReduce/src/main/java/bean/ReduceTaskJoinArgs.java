package bean;

import java.io.Serializable;

public class ReduceTaskJoinArgs implements TaskArgs, Serializable {
    private int workerId;
    private int rIndex;
    private Class<?>[] paramTypes = new Class[]{ReduceTaskJoinArgs.class, ReduceTaskJoinReply.class};

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    public int getRIndex() {
        return rIndex;
    }

    public void setRIndex(int rIndex) {
        this.rIndex = rIndex;
    }
}
