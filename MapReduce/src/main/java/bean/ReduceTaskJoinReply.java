package bean;

import java.io.Serializable;

public class ReduceTaskJoinReply implements TaskReply, Serializable {
    private boolean accept;

    public boolean isAccept() {
        return accept;
    }

    public void setAccept(boolean accept) {
        this.accept = accept;
    }
}
