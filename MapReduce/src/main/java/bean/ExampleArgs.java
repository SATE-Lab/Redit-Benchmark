package bean;

import java.io.Serializable;

// example to show how to declare the arguments and reply for an RPC.
public class ExampleArgs implements TaskArgs, Serializable {
    private static final long serialVersionUID = 1L;
    private int x;

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }
}
