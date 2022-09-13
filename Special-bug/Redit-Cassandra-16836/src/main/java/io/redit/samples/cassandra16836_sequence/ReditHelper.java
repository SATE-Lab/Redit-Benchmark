package io.redit.samples.cassandra16836_sequence;

import io.redit.ReditRunner;
import io.redit.dsl.entities.Deployment;
import io.redit.dsl.entities.PathAttr;
import io.redit.dsl.entities.ServiceType;
import io.redit.exceptions.RuntimeEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ReditHelper {
    public static final Logger logger = LoggerFactory.getLogger(ReditHelper.class);
    public static int numOfServers = 2;
    public static final int RPC_PORT = 9160;
    public static final String dir = "apache-cassandra-3.11.6";

    public static String getCassandraHomeDir(){
        return "/cassandra/" + dir;
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String compressedPath = workDir + "/../../Benchmark/cassandra-3.11.6/cassandra-3.11.6-build/cassandra-dist/target/" + dir + ".tar.gz";

        Deployment.Builder builder = Deployment.builder("sample-cassandra")
                .withService("cassandra")
                .applicationPath(compressedPath, "/cassandra",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/cassandra:3.11.6").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getCassandraHomeDir() + "/lib/*.jar").libraryPath(getCassandraHomeDir() + "/bin/*")
                .logDirectory(getCassandraHomeDir() + "/logs").serviceType(ServiceType.JAVA).and();

        builder.withService("server", "cassandra").tcpPort(RPC_PORT).and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1").applicationPath("conf/server1/cassandra.yaml", getCassandraHomeDir() + "/conf/cassandra.yaml").and()
                .node("server2").applicationPath("conf/server2/cassandra.yaml", getCassandraHomeDir() + "/conf/cassandra.yaml").and();

        builder.node("server1").and().testCaseEvents("t1", "t2", "t3").runSequence("t1 * t2 * t3");

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
