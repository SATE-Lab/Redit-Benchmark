package io.redit.samples.benchmark.raftJava;

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
    public static int numOfClients = 2;
    private static final int HTTP_PORT = 8051;
    public static final int RPC_PORT = 8052;

    public static String getRaftHomeDir(){
        return "/raft-java/raft-java-1.9.0";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String dir = "raft-java-1.9.0";
        Deployment.Builder builder = Deployment.builder("sample-raft-java")
                .withService("raft-java")
                .applicationPath(workDir + "/../Benchmark/Raft-Java/raft-java-1.9.0-build/raft-java-dist/target/" + dir + ".tar.gz", "/raft-java",  PathAttr.COMPRESSED)
//                .applicationPath(workDir + "/../Benchmark/Raft-Java-sleep/raft-java-1.9.0-build/raft-java-dist/target/" + dir + ".tar.gz", "/raft-java",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/raft-java:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getRaftHomeDir() + "/lib/*.jar")
                .logDirectory(getRaftHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("server", "raft-java").tcpPort(HTTP_PORT, RPC_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .withService("client", "raft-java").tcpPort(HTTP_PORT, RPC_PORT).and()
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
