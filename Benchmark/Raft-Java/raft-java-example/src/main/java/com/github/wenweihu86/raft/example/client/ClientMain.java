package com.github.wenweihu86.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMain {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.printf("Usage: ./run_client.sh CLUSTER KEY [VALUE]\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }

        // init rpc client
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat jsonFormat = new JsonFormat();

        // set
        if (value != null) {
            ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                    .setKey(key).setValue(value).build();
            ExampleProto.SetResponse setResponse = exampleService.set(setRequest);

            LOG.info("set request, key="+ key +" value="+ value +", response=" + jsonFormat.printToString(setResponse));
        } else {
            // get
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            LOG.info("get request, key="+ key + ", response" + jsonFormat.printToString(getResponse));
        }

        rpcClient.stop();
    }
}
