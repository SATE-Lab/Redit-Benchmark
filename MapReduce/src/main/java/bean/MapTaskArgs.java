package bean;

import java.io.Serializable;

public class MapTaskArgs implements TaskArgs, Serializable {
    // -1 if does not have one
    private int workerId;
    private Class<?>[] paramTypes = new Class[]{MapTaskArgs.class, MapTaskReply.class};

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
