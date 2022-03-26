package mr;

import bean.TaskArgs;
import bean.TaskReply;

import java.io.*;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.List;

public class ServerService implements Runnable{
    private Socket sockClient;
    private Class serviceRegistryClass;
    private Coordinator coordinator;

    public ServerService(Socket sock, Coordinator coordinator) {
        super();
        this.sockClient = sock;
        this.coordinator = coordinator;
    }

    public void registerService(Class c) {
        this.serviceRegistryClass = c;
    }

    @Override
    public void run() {
        try {
            // after the connection is established, the input stream is obtained from the socket
            // and a buffer is established for reading.
            OutputStream out = sockClient.getOutputStream();
            InputStream in = sockClient.getInputStream();
            ObjectInputStream inputStream = new ObjectInputStream(in);
            ObjectOutputStream outputStream = new ObjectOutputStream(out);

            // get the request data and force the parameter type
            List<Object> list = (List<Object>) inputStream.readObject();
            String rpcName = (String) list.get(0);
            Class<?>[] paramTypes = (Class<?>[]) list.get(1);
            TaskArgs args = (TaskArgs) list.get(2);
            TaskReply reply = (TaskReply) list.get(3);

            // find and execute service methods
            if(serviceRegistryClass != null) {
                Method method = serviceRegistryClass.getMethod(rpcName, paramTypes);
                Object[] objects = new Object[]{args, reply};
                reply = (TaskReply) method.invoke(coordinator, objects);
            }
            outputStream.writeObject(reply);
            outputStream.flush();
            out.close();
            in.close();
            sockClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
