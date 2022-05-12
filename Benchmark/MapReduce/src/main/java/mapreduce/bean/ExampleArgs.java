package mapreduce.bean;

import java.io.Serializable;

// example to show how to declare the arguments and reply for an RPC.
public class ExampleArgs implements TaskArgs, Serializable {
    private static final long serialVersionUID = 1L;
    private Class<?>[] paramTypes = new Class[]{ExampleArgs.class, ExampleReply.class};
    private int x;

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }
}
