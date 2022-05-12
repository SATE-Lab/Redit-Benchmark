package mapreduce.bean;

import java.io.Serializable;

// example to show how to declare the arguments and reply for an RPC.
public class ExampleReply implements TaskReply, Serializable {
    private static final long serialVersionUID = 1L;
    private int y;

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}
