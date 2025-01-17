package io.redit.samples.benchmark.elasticsearch19269;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import io.redit.execution.NetPart;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SampleTest {

    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ElasticsearchYmlFile = "elasticsearch.yml";
    private static String YmlConfig1 = "discovery.zen.ping.unicast.hosts: [";
    private static String YmlConfig2 = "network.publish_host: ";
    private String masterServer;

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
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        startServers();
        checkJps();
        checkElasticsearchStatus();
        refreshMasterServer();
        testDirtyRead();
        logger.info("test completed");
    }

    private void refreshMasterServer() throws RuntimeEngineException {
        String command = "curl localhost:9200/_cat/master";
        String commandResult = getCommandResult(runner.runtime().runCommandInNode("server1", command));
        assert commandResult != null && commandResult.length() != 0;
        String[] commandResList = commandResult.split("\\s");
        for (String s : commandResList) {
            if (s.startsWith("node")) {
                this.masterServer = "server" + s.substring(s.length() - 1);
                logger.info("Current master: " + s + ". Ip address: " + runner.runtime().ip(this.masterServer));
            }
        }
        assert this.masterServer.equals("server1");
    }

    private void testDirtyRead() throws RuntimeEngineException, InterruptedException {
        String createIndexCmd = "curl  -XPUT 'localhost:9200/foo?pretty'";
        String queryIndexCmd = "curl 'localhost:9200/_cat/indices?v'";
        String createDocCmd = "curl -XPUT 'localhost:9200/foo/bar/1?pretty' -d '{ \"value\": \"origin\" }'";
        String updateDocCmd1 = "curl --connect-timeout 5 -m 5 -XPOST 'localhost:9200/foo/bar/1/_update?pretty' -d '{\"doc\": { \"value\": \"dirty value\" }}'";
        String updateDocCmd2 = "curl --connect-timeout 5 -m 5 -XPOST 'localhost:9200/foo/bar/1/_update?pretty' -d '{\"doc\": { \"value\": \"something else\" }}'";
        String queryShardCmd = "curl localhost:9200/_cat/shards";
        String primaryShardServer = null;
        String replicaShardServer = null;
        printResult(runner.runtime().runCommandInNode("server1", createIndexCmd));
        printResult(runner.runtime().runCommandInNode("server1", queryIndexCmd));
        printResult(runner.runtime().runCommandInNode("server1", createDocCmd));

        Thread.sleep(5000);

        // 筛选出主分片所在server
        String shardStr = getCommandResult(runner.runtime().runCommandInNode("server1", queryShardCmd));
        logger.info("shardInfo: \n" + shardStr);
        String[] shardStrList = shardStr.split("\n");
        for (String shardInfo : shardStrList) {
            if (shardInfo.contains(" p ")) {
                String[] primaryShardInfoList = shardInfo.trim().split(" ");
                String primaryShardNodeName = primaryShardInfoList[primaryShardInfoList.length - 1];
                String primaryShardIpAddress = primaryShardInfoList[primaryShardInfoList.length - 2];
                primaryShardServer = "server" + primaryShardNodeName.substring(primaryShardNodeName.length() - 1);
                logger.info(String.format("PrimaryShardServer: %s; Ip address: %s", primaryShardServer, primaryShardIpAddress));
            }
        }
        assert primaryShardServer != null && primaryShardServer.length() != 0;

        // 进行网络分区
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {  // 筛选出非primary shard的两台服务器
            String serverName = "server" + i;
            if (!serverName.equals(primaryShardServer) && !serverName.equals(this.masterServer)) {
                replicaShardServer = serverName;
            }
        }

        NetPart netPart = NetPart.partitions(primaryShardServer, this.masterServer + "," + replicaShardServer).build();
        logger.info("--> start disrupting network");
        runner.runtime().networkPartition(netPart);
        Thread.sleep(3000);

        // 在primaryShardServer上创建值并尝试脏读,期望读取到“dirty value”
        printResult(runner.runtime().runCommandInNode(primaryShardServer, updateDocCmd1));
        boolean dirtyReadSuccess = attemptToGetQueryResponse(primaryShardServer);
        if(!dirtyReadSuccess){
            Assert.fail("dirty read failed (first read failed)");
        }

        // 网络分区未结束时，在master重新写入
        Thread.sleep(10000);
        printResult(runner.runtime().runCommandInNode(this.masterServer, updateDocCmd2));

        // 撤销网络分区，在master上读取，期望读取到“something else”
        runner.runtime().removeNetworkPartition(netPart);
        Thread.sleep(10000);

        boolean secondReadSuccess = attemptToGetQueryResponse(this.masterServer);
        if(!secondReadSuccess){
            Assert.fail("second read failed");
        }

    }

    private boolean attemptToGetQueryResponse(String serverName) throws RuntimeEngineException, InterruptedException {
        String queryDocCmd = "curl --connect-timeout 5 -m 5 -XGET 'localhost:9200/foo/bar/1?pretty'";
        int maxAttempt = 5;
        for (int i = 1; i <= maxAttempt; i++) { // 进行数次轮询，若取得期望结果或超出轮询次数则退出
            String queryDocCmdRes  = getCommandResult(runner.runtime().runCommandInNode(serverName, queryDocCmd));
            if(queryDocCmdRes.contains("_version")){
                logger.info(queryDocCmdRes);
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }


    private void startServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            startServer(i);
        }
    }

    private void startServer(int serverId) throws RuntimeEngineException, InterruptedException {

        logger.info("server" + serverId + " startServer...");
        printResult(runner.runtime().runCommandInNode("server" + serverId, "useradd test && chown -R test /elasticsearch && chown -R test /var"));
        Thread.sleep(500);
        String command = "runuser -m test -c '" + ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch -d'";
        printResult(runner.runtime().runCommandInNode("server" + serverId, command));
        Thread.sleep(4000);
    }


    private static void getYmlConf() {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            YmlConfig1 += "\"" + runner.runtime().ip("server" + (i)) + ":9300\"";
            if (i < ReditHelper.numOfServers) {
                YmlConfig1 += ", ";
            }
        }
        YmlConfig1 += "]";
    }


    private static void addElasticsearchYmlFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + ElasticsearchYmlFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("discovery.zen.ping.unicast.hosts")) {
                addConf = true;
            }
        }
        in.close();
        if (addConf) {
            logger.info("already add config in " + ElasticsearchYmlFile);
        } else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++) {
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + ElasticsearchYmlFile, true));
                out.write(YmlConfig2 + runner.runtime().ip("server" + i) + "\n");
                out.write(YmlConfig1);
                out.close();
            }
        }
    }

    private void checkJps() throws RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
        }
    }

    private void checkElasticsearchStatus() throws RuntimeEngineException {
        String cmd = "curl -X GET http://localhost:9200/?pretty";
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            printResult(runner.runtime().runCommandInNode("server" + i, cmd));
        }
    }

    private static String getCommandResult(CommandResults commandResults) {
        if (commandResults.stdOut() != null && commandResults.stdOut().length() != 0) {
            return commandResults.stdOut();
        } else {
            return commandResults.stdErr();
        }
    }

    private static void printResult(CommandResults commandResults) {
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        logger.info("\n" + getCommandResult(commandResults));
    }
}
