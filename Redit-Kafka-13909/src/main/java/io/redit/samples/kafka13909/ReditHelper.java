package io.redit.samples.kafka13909;

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

    public static String getKafkaHomeDir(){
        return "/kafka/kafka_2.13-2.8.0";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String KafkaDir = "kafka_2.13-2.8.0";
        Deployment.Builder builder = Deployment.builder("sample-kafka")
                .withService("kafka")
                .applicationPath(workDir + "/../Benchmark/kafka-3.2.0/kafka-3.2.0-build/kafka-dist/target/" + KafkaDir + ".tar.gz", "/kafka",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/kafka:2.8.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getKafkaHomeDir() + "/bin/*.sh")
                .libraryPath(getKafkaHomeDir() + "/libs/*.jar")
                .logDirectory(getKafkaHomeDir() + "/logs")
                .serviceType(ServiceType.JAVA).and();

        builder.withService("server", "kafka").and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1")
                .applicationPath("conf/server1/server.properties", getKafkaHomeDir() + "/config/server.properties").and()
                .node("server2")
                .applicationPath("conf/server2/server.properties", getKafkaHomeDir() + "/config/server.properties").and()
                .node("server3")
                .applicationPath("conf/server3/server.properties", getKafkaHomeDir() + "/config/server.properties").and();

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
