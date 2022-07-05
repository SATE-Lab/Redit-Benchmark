package com.github.wenweihu86.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ConcurrentClientMain {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentClientMain.class);
    private static JsonFormat jsonFormat = new JsonFormat();

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.printf("Usage: ./run_concurrent_client.sh THREAD_NUM\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);

        ExecutorService readThreadPool = Executors.newFixedThreadPool(3);
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(3);
        Future<?>[] future = new Future[3];
        for (int i = 0; i < 3; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool));
        }
    }

    public static class SetTask implements Runnable {
        private ExampleService exampleService;
        ExecutorService readThreadPool;

        public SetTask(ExampleService exampleService, ExecutorService readThreadPool) {
            this.exampleService = exampleService;
            this.readThreadPool = readThreadPool;
        }

        @Override
        public void run() {
            while (true) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();

                long startTime = System.currentTimeMillis();
                ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
                try {
                    if (setResponse != null) {
                        LOG.info("set request, key="+ key +", value="+ value +", response="+ jsonFormat.printToString(setResponse));
                        readThreadPool.submit(new GetTask(exampleService, key));
                    } else {
                        LOG.info("set request failed, key="+ key +" value=" + value);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private String key;

        public GetTask(ExampleService exampleService, String key) {
            this.exampleService = exampleService;
            this.key = key;
        }

        @Override
        public void run() {
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            try {
                if (getResponse != null) {
                    LOG.info("get request, key="+ key  +", response="+ jsonFormat.printToString(getResponse));
                } else {
                    LOG.info("get request failed, key=" + key);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
