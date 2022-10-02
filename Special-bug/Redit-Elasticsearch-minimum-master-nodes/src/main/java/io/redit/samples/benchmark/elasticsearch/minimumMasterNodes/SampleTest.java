package io.redit.samples.benchmark.elasticsearch.minimumMasterNodes;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SampleTest {

    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ElasticsearchYmlFile = "elasticsearch.yml";
    private static String ElasticsearchYmlConf = "discovery.seed_hosts: [";
    private static int connectTimeout = 10;
    private static int dataTransferTimeout = 15;

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        getYmlConf();
        addElasticsearchYmlFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, IOException, RuntimeEngineException {
        logger.info("wait for Elasticsearch...");
        startServers();
        Thread.sleep(5000);
        testMinimumMasterNodesMoreThanCurrentNodeCnt();
        logger.info("completed !!!");
    }

    private void testMinimumMasterNodesMoreThanCurrentNodeCnt() throws RuntimeEngineException, InterruptedException {
        logger.info("discovery.zen.minimum_master_nodes:3, master eligible node: 3");
        createAnIndex(1);
        runner.runtime().killNode("server2");
        logger.info("discovery.zen.minimum_master_nodes:3, master eligible node: 2");
        Thread.sleep(2000);
        checkServerStatus(1); // status expected to be 503
    }

    // 3个节点都正常，尝试创建一个index
    private void createAnIndex(int serverId) throws RuntimeEngineException {
        String createCommand = String.format("curl --connect-timeout %d -m %d -XPUT 'localhost:9200/customer?pretty'", connectTimeout, dataTransferTimeout);
        String catCommand = String.format("curl --connect-timeout %d -m %d 'localhost:9200/_cat/indices?v'", connectTimeout, dataTransferTimeout);
        CommandResults r1 = runner.runtime().runCommandInNode("server" + serverId, createCommand);
        printResult(r1);
        CommandResults r2 = runner.runtime().runCommandInNode("server" + serverId, catCommand);
        printResult(r2);
    }

    // 只有2个节点，对其中任意一个轮询状态，status 一直保持为503，且日志中会有 “not enough master nodes”
    private void checkServerStatus(int serverId) throws RuntimeEngineException, InterruptedException {
        String cmd = "curl -X GET http://localhost:9200/?pretty";
        for(int i=0;i<6;i++){
            printResult(runner.runtime().runCommandInNode("server" + serverId, cmd));
            Thread.sleep(5000);
        }
    }

    private void startServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            startServer(i);
            Thread.sleep(1000);
            checkJps();
        }
    }

    private void startServer(int serverId) throws RuntimeEngineException, InterruptedException {
        // 该版本可在root帐户下运行
        logger.info("server" + serverId + " startServer...");
        Thread.sleep(1000);
        String command = ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch -d";
        printResult(runner.runtime().runCommandInNode("server" + serverId, command));
        Thread.sleep(5000);
        printResult(runner.runtime().runCommandInNode("server" + serverId, "curl -X GET http://localhost:9200/?pretty"));
        Thread.sleep(500);
    }

    private static void getYmlConf() {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            ElasticsearchYmlConf += "\"" + runner.runtime().ip("server" + (i)) + ":9300\"";
            if (i < ReditHelper.numOfServers) {
                ElasticsearchYmlConf += ", ";
            }
        }
        ElasticsearchYmlConf += "]";
    }

    private static void addElasticsearchYmlFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + ElasticsearchYmlFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("discovery.seed_hosts")) {
                addConf = true;
            }
        }
        in.close();
        if (addConf) {
            logger.info("already add config in " + ElasticsearchYmlFile);
        } else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++) {
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + ElasticsearchYmlFile, true));
                out.write(ElasticsearchYmlConf);
                out.close();
                logger.info("add config to " + ElasticsearchYmlFile + ":\n" + ElasticsearchYmlConf);
            }
        }
    }

    private void checkJps() throws RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
        }
    }

    private static void printResult(CommandResults commandResults) {
        logger.info(commandResults.nodeName() + ": " + commandResults.command());

        if (commandResults.stdOut() != null && commandResults.stdOut().length() != 0) {
            logger.info(commandResults.stdOut());
        } else {
            logger.warn(commandResults.stdErr());
        }
    }
}
