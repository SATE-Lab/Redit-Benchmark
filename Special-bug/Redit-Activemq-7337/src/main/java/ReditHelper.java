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
    public static final String dir = "activemq-5.15.9";

    public static String getRocketmq1HomeDir(){
        return "/activemq/" + dir + "/mq1";
    }
    public static String getRocketmq2HomeDir(){
        return "/activemq/" + dir + "/mq2";
    }
    public static String getRocketmq3HomeDir(){
        return "/activemq/" + dir + "/mq3";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String compressedPath = workDir + "/../../Benchmark/activemq-5.16.5/activemq-5.16.5-build/activemq-5.16.5-dist/target/" + dir + ".tar.gz";

        Deployment.Builder builder = Deployment.builder("sample-activemq")
                .withService("activemq")
                .applicationPath(compressedPath, "/activemq",  PathAttr.COMPRESSED)
                .dockerImageName("mengpo1106/activemq:1.0").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getRocketmq1HomeDir() + "/lib/*.jar").libraryPath(getRocketmq1HomeDir() + "/bin/*")
                .libraryPath(getRocketmq2HomeDir() + "/lib/*.jar").libraryPath(getRocketmq2HomeDir() + "/bin/*")
                .libraryPath(getRocketmq3HomeDir() + "/lib/*.jar").libraryPath(getRocketmq3HomeDir() + "/bin/*")
                .logDirectory(getRocketmq1HomeDir() + "/data")
                .logDirectory(getRocketmq2HomeDir() + "/data")
                .logDirectory(getRocketmq3HomeDir() + "/data")
                .serviceType(ServiceType.JAVA).and();

        builder.withService("server", "activemq").and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1").applicationPath("conf/server1/mq1/activemq.xml", getRocketmq1HomeDir() + "/conf/activemq.xml").and()
                .node("server1").applicationPath("conf/server1/mq1/jetty.xml", getRocketmq1HomeDir() + "/conf/jetty.xml").and()
                .node("server1").applicationPath("conf/server1/mq2/activemq.xml", getRocketmq2HomeDir() + "/conf/activemq.xml").and()
                .node("server1").applicationPath("conf/server1/mq2/jetty.xml", getRocketmq2HomeDir() + "/conf/jetty.xml").and()
                .node("server1").applicationPath("conf/server1/mq3/activemq.xml", getRocketmq3HomeDir() + "/conf/activemq.xml").and()
                .node("server1").applicationPath("conf/server1/mq3/jetty.xml", getRocketmq3HomeDir() + "/conf/jetty.xml").and()
                .node("server2").applicationPath("conf/server2/mq1/activemq.xml", getRocketmq1HomeDir() + "/conf/activemq.xml").and()
                .node("server2").applicationPath("conf/server2/mq1/jetty.xml", getRocketmq1HomeDir() + "/conf/jetty.xml").and()
                .node("server2").applicationPath("conf/server2/mq2/activemq.xml", getRocketmq2HomeDir() + "/conf/activemq.xml").and()
                .node("server2").applicationPath("conf/server2/mq2/jetty.xml", getRocketmq2HomeDir() + "/conf/jetty.xml").and()
                .node("server2").applicationPath("conf/server2/mq3/activemq.xml", getRocketmq3HomeDir() + "/conf/activemq.xml").and()
                .node("server2").applicationPath("conf/server2/mq3/jetty.xml", getRocketmq3HomeDir() + "/conf/jetty.xml").and();

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
