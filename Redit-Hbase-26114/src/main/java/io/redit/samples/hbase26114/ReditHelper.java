package io.redit.samples.hbase26114;

import io.redit.ReditRunner;
import io.redit.dsl.entities.Deployment;
import io.redit.dsl.entities.PathAttr;
import io.redit.dsl.entities.ServiceType;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ReditHelper {
    public static final Logger logger = LoggerFactory.getLogger(ReditHelper.class);
    private static final int NN_HTTP_PORT = 50070;
    private static final int NN_RPC_PORT = 8020;
    private static int numOfNNs = 3;
    private static int numOfDNs = 3;
    private static int numOfJNs = 3;
    public static int numOfServers = 3;

    public static String getHadoopHomeDir(){
        return "/hadoop/hadoop-3.3.1";
    }
    public static String getZookeeperHomeDir(){
        return "/zookeeper/apache-zookeeper-3.7.1-bin";
    }
    public static String getHbaseHomeDir(){
        return "/hbase/hbase-2.2.2";
    }

    public static Deployment getDeployment() {

        String workDir = System.getProperty("user.dir");
        String HadoopDir = "hadoop-3.3.1";
        String ZookeeperDir = "apache-zookeeper-3.7.1-bin";
        String HbaseDir = "hbase-2.2.2";
        Deployment.Builder builder = Deployment.builder("sample-hbase")
                .withService("hadoop-base")
                .applicationPath(workDir + "/../Benchmark/MapReduce/hadoop-3.3.1-build/hadoop-dist/target/" + HadoopDir + ".tar.gz", "/hadoop",  PathAttr.COMPRESSED)
                .applicationPath("conf/hdfs-site.xml", getHadoopHomeDir() + "/etc/hadoop/hdfs-site.xml")
                .applicationPath("conf/core-site.xml", getHadoopHomeDir() + "/etc/hadoop/core-site.xml")
                .environmentVariable("HADOOP_HOME", getHadoopHomeDir()).environmentVariable("HADOOP_HEAPSIZE_MAX", "1g")
                .dockerImageName("mengpo1106/hadoop:3.3.1").dockerFileAddress("docker/Dockerfile", true)
                .libraryPath(getHadoopHomeDir() + "/bin/*.sh")
                .libraryPath(getHadoopHomeDir() + "/sbin/*.sh")
                .libraryPath(getHadoopHomeDir() + "/lib/*.jar")
                .libraryPath(getHadoopHomeDir() + "/share/hadoop/**/*.jar")
                .libraryPath(getHadoopHomeDir() + "/share/hadoop/**/*.java")
                .logDirectory(getHadoopHomeDir() + "/logs")
                .serviceType(ServiceType.JAVA).and();

        builder.withService("hbase")
                .applicationPath(workDir + "/../Benchmark/zookeeper-3.7.1/zookeeper-3.7.1-build/zookeeper-dist/target/" + ZookeeperDir + ".tar.gz", "/zookeeper",  PathAttr.COMPRESSED)
                .applicationPath("conf/zoo.cfg", getZookeeperHomeDir() + "/conf/zoo.cfg")
                .applicationPath(workDir + "/../Benchmark/hbase-2.4.12/hbase-2.4.12-build/hbase-dist/target/" + HbaseDir + ".tar.gz", "/hbase",  PathAttr.COMPRESSED)
                .applicationPath("conf/hbase-env.sh", getHbaseHomeDir() + "/conf/hbase-env.sh")
                .applicationPath("conf/hbase-site.xml", getHbaseHomeDir() + "/conf/hbase-site.xml")
                .applicationPath("conf/regionservers", getHbaseHomeDir() + "/conf/regionservers")
                .applicationPath("conf/hdfs-site.xml", getHbaseHomeDir() + "/conf/hdfs-site.xml")
                .applicationPath("conf/core-site.xml", getHbaseHomeDir() + "/conf/core-site.xml")
                .applicationPath("conf/hosts", "/etc/hosts")
                .dockerImageName("mengpo1106/hbase:2.2.2").dockerFileAddress("docker/Dockerfile", true)
                .environmentVariable("HADOOP_HOME", getHadoopHomeDir()).environmentVariable("HADOOP_HEAPSIZE_MAX", "1g")
                .environmentVariable("HBASE_HOME", getHbaseHomeDir())
                .libraryPath(getZookeeperHomeDir() + "/bin/*.sh")
                .libraryPath(getZookeeperHomeDir() + "/lib/*.jar")
                .libraryPath(getHbaseHomeDir() + "/bin/*.sh")
                .libraryPath(getHbaseHomeDir() + "/lib/*.jar")
                .logDirectory(getZookeeperHomeDir() + "/logs")
                .logDirectory(getHbaseHomeDir() + "/logs")
                .serviceType(ServiceType.JAVA).and();

        builder.withService("nn", "hadoop-base").tcpPort(NN_HTTP_PORT, NN_RPC_PORT)
                .initCommand(getHadoopHomeDir() + "/bin/hdfs namenode -bootstrapStandby")
                .startCommand(getHadoopHomeDir() + "/bin/hdfs --daemon start namenode")
                .stopCommand(getHadoopHomeDir() + "/bin/hdfs --daemon stop namenode").and()
                .nodeInstances(numOfNNs, "nn", "nn", true)
                .withService("dn", "hadoop-base")
                .startCommand(getHadoopHomeDir() + "/bin/hdfs --daemon start datanode")
                .stopCommand(getHadoopHomeDir() + "/bin/hdfs --daemon stop datanode").and()
                .nodeInstances(numOfDNs, "dn", "dn", true)
                .withService("jn", "hadoop-base")
                .startCommand(getHadoopHomeDir() + "/bin/hdfs --daemon start journalnode")
                .stopCommand(getHadoopHomeDir() + "/bin/hdfs --daemon stop journalnode").and()
                .nodeInstances(numOfJNs, "jn", "jn", false);

        builder.node("nn1").initCommand(getHadoopHomeDir() + "/bin/hdfs namenode -format").and();
        addRuntimeLibsToDeployment(builder, getHadoopHomeDir());
        addInstrumentablePath(builder, "/share/hadoop/hdfs/hadoop-hdfs-3.3.1.jar");

        builder.withService("server", "hbase").and()
                .nodeInstances(numOfServers, "server", "server", true)
                .node("server1")
                .applicationPath("conf/server1/myid", getZookeeperHomeDir() + "/zkdata/myid").and()
                .node("server2")
                .applicationPath("conf/server2/myid", getZookeeperHomeDir() + "/zkdata/myid").and()
                .node("server3")
                .applicationPath("conf/server3/myid", getZookeeperHomeDir() + "/zkdata/myid").and();

        return builder.build();
    }

    public static void startNodesInOrder(ReditRunner runner) throws RuntimeEngineException {
        try {
            if (numOfNNs > 1) {
                // wait for journal nodes to come up
                Thread.sleep(10000);
            }
            runner.runtime().startNode("nn1");
            Thread.sleep(15000);
            if (numOfNNs > 1) {
                for (int nnIndex=2; nnIndex<=numOfNNs; nnIndex++) {
                    runner.runtime().startNode("nn" + nnIndex);
                }
            }
            for (String node : runner.runtime().nodeNames())
                if (node.startsWith("dn")) runner.runtime().startNode(node);

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

    private static void addRuntimeLibsToDeployment(Deployment.Builder builder, String hadoopHome) {
        for (String cpItem: System.getProperty("java.class.path").split(":")) {
            if (cpItem.contains("aspectjrt") || cpItem.contains("reditrt")) {
                String fileName = new File(cpItem).getName();
                logger.info("addRuntimeLibsToDeployment: " + hadoopHome + "/share/hadoop/common/" + fileName);
                builder.service("hadoop-base")
                        .applicationPath(cpItem, hadoopHome + "/share/hadoop/common/" + fileName, PathAttr.LIBRARY).and();
            }
        }
    }

    public static void addInstrumentablePath(Deployment.Builder builder, String path) {
        String[] services = {"hadoop-base", "nn", "dn", "jn"};
        for (String service: services) {
            builder.service(service).instrumentablePath(getHadoopHomeDir() + path).and();
        }
    }

    public static void waitActive() throws RuntimeEngineException {

        for (int index=1; index<= numOfNNs; index++) {
            for (int retry=3; retry>0; retry--){
                logger.info("Checking if NN nn{} is UP (retries left {})", index, retry-1);
                if (assertNNisUpAndReceivingReport(index, numOfDNs))
                    break;

                if (retry > 1) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.warn("waitActive sleep got interrupted");
                    }
                } else {
                    throw new RuntimeException("NN nn" + index + " is not active or not receiving reports from DNs");
                }
            }
        }
        logger.info("The cluster is ACTIVE");
    }

    public static boolean assertNNisUpAndReceivingReport(int index, int numOfDNs) throws RuntimeEngineException {
        if (!isNNUp(index))
            return false;

        String res = getNNJmxHaInfo(index);
        if (res == null) {
            logger.warn("Error while trying to get the status of name node");
            return false;
        }

        logger.info("NN {} is up. Checking datanode connections", "nn" + index);
        return res.contains("\"NumLiveDataNodes\" : " + numOfDNs);
    }

    public static boolean isNNUp(int index) throws RuntimeEngineException {
        String res = getNNJmxHaInfo(index);
        if (res == null) {
            logger.warn("Error while trying to get the status of name node");
            return false;
        }

        return res.contains("\"tag.HAState\" : \"active\"") || res.contains("\"tag.HAState\" : \"standby\"");
    }

    private static String getNNJmxHaInfo(int index) {
        OkHttpClient client = new OkHttpClient();
        try {
            return client.newCall(new Request.Builder()
                    .url("http://" + SampleTest.runner.runtime().ip("nn" + index) + ":" + NN_HTTP_PORT +
                            "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem")
                    .build()).execute().body().string();
        } catch (IOException e) {
            logger.warn("Error while trying to get the status of name node");
            return null;
        }
    }

    public static void transitionToActive(int nnNum, ReditRunner runner) throws RuntimeEngineException {
        logger.info("Transitioning nn{} to ACTIVE", nnNum);
        CommandResults res = runner.runtime().runCommandInNode("nn" + nnNum, getHadoopHomeDir() + "/bin/hdfs haadmin -transitionToActive nn" + nnNum);
        if (res.exitCode() != 0) {
            throw new RuntimeException("Error while transitioning nn" + nnNum + " to ACTIVE.\n" + res.stdErr());
        }
    }

    public static void checkNNs(ReditRunner runner) throws RuntimeEngineException {
        logger.info("start check NNs !!!");
        for(int nnNum = 1; nnNum <= numOfNNs; nnNum++){
            CommandResults res = runner.runtime().runCommandInNode("nn" + nnNum, getHadoopHomeDir() + "/bin/hdfs haadmin -getServiceState nn" + nnNum);
            logger.info("nn" + nnNum + " status: " + res.stdOut());
        }
    }
}
