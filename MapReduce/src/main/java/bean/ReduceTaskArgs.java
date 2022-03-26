package bean;

import java.io.Serializable;

public class ReduceTaskArgs implements TaskArgs, Serializable {
    private int workerId;
    private Class<?>[] paramTypes = new Class[]{ReduceTaskArgs.class, ReduceTaskReply.class};

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }
}
