package io.redit.samples.benchmark.jlitespider;

import io.redit.ReditRunner;
import io.redit.dsl.entities.Deployment;
import io.redit.dsl.entities.PathAttr;
import io.redit.dsl.entities.ServiceType;
import io.redit.exceptions.RuntimeEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReditHelper {
    public static final Logger logger = LoggerFactory.getLogger(ReditHelper.class);
    public static int numOfMq = 1;
    public static int numOfSpider = 2;
    public static int numOfLighter = 2;
    public static final int SERVER_PORT = 5672;

    public static String getHomeDir(){
        return "/jlitespider/jlitespider-1.0.0";
    }

    public static Deployment getDeployment() {
        String workDir = System.getProperty("user.dir");
        String dir = "jlitespider-1.0.0";
        Deployment.Builder builder = Deployment.builder("sample-rabbitmq")
                .withService("rabbitmq")
                .applicationPath("etc/rabbitmq/rabbitmq.config","/etc/rabbitmq/rabbitmq.config")
                .dockerImageName("mengpo1106/jlitespider-rabbitmq:1.0").dockerFileAddress("docker/rabbitmq", true)
                .serviceType(ServiceType.JAVA).and()

                .withService("jlitespider")
                .applicationPath(workDir + "/../Benchmark/JLiteSpider/jlitespider-1.0.0-build/jlitespider-dist/target/" + dir + ".tar.gz", "/jlitespider",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/jlitespider:1.0").dockerFileAddress("docker/jlitespider", true)
                .libraryPath(getHomeDir() + "/lib/*.jar").logDirectory(getHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("rabbitmq", "rabbitmq").tcpPort(SERVER_PORT).and()
                .nodeInstances(numOfMq, "rabbitmq", "rabbitmq", true)
                .withService("spider", "jlitespider").and()
                .nodeInstances(numOfSpider, "spider", "spider", true)
                .withService("lighter", "jlitespider").and()
                .nodeInstances(numOfLighter, "lighter", "lighter", true);;

        return builder.build();
    }

    public static void startNodesInOrder(ReditRunner runner) throws RuntimeEngineException {
        try {
            runner.runtime().startNode("rabbitmq1");
            Thread.sleep(1000);
            if (numOfMq > 1) {
                for (int Index = 2; Index <= numOfMq; Index++) {
                    runner.runtime().startNode("rabbitmq" + Index);
                }
            }
            for (String node : runner.runtime().nodeNames())
                if (node.startsWith("spider")) runner.runtime().startNode(node);
            for (String node : runner.runtime().nodeNames())
                if (node.startsWith("lighter")) runner.runtime().startNode(node);
        } catch (InterruptedException e) {
            logger.warn("startNodesInOrder sleep got interrupted");
        }
    }
}
