package io.redit.samples.benchmark.elasticsearch;

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
    private static final int HTTP_PORT = 9200;

    public static String getElasticsearchHomeDir(){
        return "/elasticsearch/elasticsearch-8.2.2";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String dir = "elasticsearch-8.2.2";
        Deployment.Builder builder = Deployment.builder("sample-elasticsearch")
                .withService("elasticsearch")
                .applicationPath(workDir + "/../Benchmark/elasticsearch-8.2.2/elasticsearch-8.2.2-build/elasticsearch-dist/target/" + dir + ".tar.gz", "/elasticsearch",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/elasticsearch:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getElasticsearchHomeDir() + "/lib/*.jar")
                .libraryPath(getElasticsearchHomeDir() + "/bin/*")
                .logDirectory(getElasticsearchHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("server", "elasticsearch").tcpPort(HTTP_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1")
                .applicationPath("conf/server1/elasticsearch.yml", getElasticsearchHomeDir() + "/config/elasticsearch.yml").and()
                .node("server2")
                .applicationPath("conf/server2/elasticsearch.yml", getElasticsearchHomeDir() + "/config/elasticsearch.yml").and()
                .node("server3")
                .applicationPath("conf/server3/elasticsearch.yml", getElasticsearchHomeDir() + "/config/elasticsearch.yml").and();


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
