package io.redit.samples.rocketmq189;

import io.redit.ReditRunner;
import io.redit.dsl.entities.Deployment;
import io.redit.dsl.entities.PathAttr;
import io.redit.dsl.entities.ServiceType;
import io.redit.exceptions.RuntimeEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReditHelper {
    public static final Logger logger = LoggerFactory.getLogger(ReditHelper.class);
    public static int numOfServers = 2;
    private static final int HTTP_PORT = 9876;
    private static final String dir = "rocketmq-4.0.0-incubating";

    public static String getRocketmqHomeDir(){
        return "/rocketmq/" + dir;
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String CompressedPath = workDir + "/../../Benchmark/rocketmq-4.9.4/rocketmq-4.9.4-build/rocketmq-4.9.4-dist/target/"+ dir + ".tar.gz";

        Deployment.Builder builder = Deployment.builder("sample-rocketmq")
                .withService("rocketmq")
                .applicationPath(CompressedPath, "/rocketmq",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/rocketmq:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getRocketmqHomeDir() + "/lib/*.jar")
                .logDirectory(getRocketmqHomeDir() + "/logs")
                .serviceType(ServiceType.JAVA).and();

        builder.withService("server", "rocketmq").tcpPort(HTTP_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1").applicationPath("conf/broker-a.properties", getRocketmqHomeDir() + "/conf/2m-2s-async/broker-a.properties").and()
                .node("server1").applicationPath("conf/broker-b-s.properties", getRocketmqHomeDir() + "/conf/2m-2s-async/broker-b-s.properties").and()
                .node("server2").applicationPath("conf/broker-b.properties", getRocketmqHomeDir() + "/conf/2m-2s-async/broker-b.properties").and()
                .node("server2").applicationPath("conf/broker-a-s.properties", getRocketmqHomeDir() + "/conf/2m-2s-async/broker-a-s.properties").and();

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
        } catch (InterruptedException e) {
            logger.warn("startNodesInOrder sleep got interrupted");
        }
    }
}
