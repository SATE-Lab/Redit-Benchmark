package io.redit.samples.benckmark.distributedid;

import io.redit.ReditRunner;
import io.redit.dsl.entities.Deployment;
import io.redit.dsl.entities.PathAttr;
import io.redit.dsl.entities.ServiceType;
import io.redit.exceptions.RuntimeEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReditHelper {
    public static final Logger logger = LoggerFactory.getLogger(ReditHelper.class);
    public static int numOfServers = 3;
    public static int numOfClients = 1;
    public static final int HTTP_PORT = 16830;
    public static final int RPC_PORT = 8052;

    public static String getHomeDir(){
        return "/distributed-id/distributed-id-1.0.0";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String dir = "distributed-id-1.0.0";
        Deployment.Builder builder = Deployment.builder("sample-distributed-id")
                .withService("distributed-id")
                .applicationPath(workDir + "/../Benchmark/Distributed-Id/distributed-id-1.0.0-build/distributed-id-dist/target/" + dir + ".tar.gz", "/distributed-id",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/distributed-id:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getHomeDir() + "/lib/*.jar").logDirectory(getHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("server", "distributed-id").tcpPort(HTTP_PORT, RPC_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .withService("client", "distributed-id").and()
                .nodeInstances(numOfClients, "client", "client", true);

        return builder.build();
    }

    public static void startNodesInOrder(ReditRunner runner) throws RuntimeEngineException {
        try {
            runner.runtime().startNode("server1");
            Thread.sleep(1000);
            if (numOfServers > 1) {
                for (int Index = 2; Index <= numOfServers; Index++) {
                    runner.runtime().startNode("server" + Index);
                }
            }
            for (String node : runner.runtime().nodeNames())
                if (node.startsWith("client")) runner.runtime().startNode(node);
        } catch (InterruptedException e) {
            logger.warn("startNodesInOrder sleep got interrupted");
        }
    }
}
