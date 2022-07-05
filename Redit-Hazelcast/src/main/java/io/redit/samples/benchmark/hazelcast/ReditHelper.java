package io.redit.samples.benchmark.hazelcast;

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
    private static final int HTTP_PORT = 5701;

    public static String getRaftHomeDir(){
        return "/hazelcast/hazelcast-5.1.2";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String dir = "hazelcast-5.1.2";
        Deployment.Builder builder = Deployment.builder("sample-hazelcast")
                .withService("hazelcast")
                .applicationPath(workDir + "/../Benchmark/hazelcast-5.1.2/hazelcast-5.1.2-build/hazelcast-dist/target/" + dir + ".tar.gz", "/hazelcast",  PathAttr.COMPRESSED)
                .applicationPath("conf/hazelcast-client.xml", getRaftHomeDir() + "/config/hazelcast-client.xml")
                .dockerImageName("mengpo1106/hazelcast:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getRaftHomeDir() + "/lib/*.jar").libraryPath(getRaftHomeDir() + "/bin/*")
                .logDirectory(getRaftHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("server", "hazelcast").tcpPort(HTTP_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1").applicationPath("conf/server1/hazelcast.xml", getRaftHomeDir() + "/config/hazelcast.xml").and()
                .node("server2").applicationPath("conf/server2/hazelcast.xml", getRaftHomeDir() + "/config/hazelcast.xml").and()
                .node("server3").applicationPath("conf/server3/hazelcast.xml", getRaftHomeDir() + "/config/hazelcast.xml").and();

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
