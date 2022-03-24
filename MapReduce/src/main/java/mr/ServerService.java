package mr;

import bean.TaskArgs;
import bean.TaskReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.List;

public class ServerService implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(ServerService.class);
    private Socket sockClient;
    private Class serviceRegistryClass;

    public ServerService(Socket sock) {
        super();
        this.sockClient = sock;
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
            TaskArgs args = (TaskArgs) list.get(1);
            TaskReply reply = (TaskReply) list.get(2);

            // find and execute service methods
            logger.info("execute methods：" + rpcName);
            if(serviceRegistryClass != null) {
                Method method = serviceRegistryClass.getMethod(rpcName);
                Object[] objects = new Object[]{args, reply};
                reply = (TaskReply) method.invoke(serviceRegistryClass.newInstance(), objects);
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
