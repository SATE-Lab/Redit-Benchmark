package io.redit.samples.benchmark.elasticsearch14671;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import io.redit.execution.NetPart;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SampleTest {

    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ElasticsearchYmlFile = "elasticsearch.yml";
    private static String ElasticsearchYmlConf = "discovery.seed_hosts: [";
    private String masterServer;
    private Map<String, RestClient> clients;

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
        startServers();
        checkJps();
        checkElasticsearchStatus();
        createLowLevelRestClients();
        refreshMasterServer();
        testStaleReplicasToBePromotedToPrimary();
    }


    private void refreshMasterServer() throws IOException {
        Request queryMasterReq = new Request("GET", "/_cat/master");
        String queryMasterRes = doRequest(clients.get("server1"), queryMasterReq);
        assert queryMasterRes != null && queryMasterRes.length() != 0;
        String[] commandResList = queryMasterRes.split("\\s");
        for (String s : commandResList) {
            if (s.startsWith("server")) {
                this.masterServer = s;
                logger.info("Current master: " + s + ". Ip address: " + runner.runtime().ip(s));
            }
        }
    }

    private void testStaleReplicasToBePromotedToPrimary() throws IOException, InterruptedException, RuntimeEngineException {
        Request createIndexReq = new Request("PUT", "/foo?pretty");
        Request createDocReq = new Request("PUT", "/foo/bar/1?pretty");
        createDocReq.setJsonEntity("{ \"value\": \"origin\" }");

        Request updateDocReq = new Request("POST", "/foo/bar/1/_update?pretty");
        updateDocReq.setJsonEntity("{\"doc\": { \"value\": \"something else\" }}");

        Request queryShardReq = new Request("GET", "/_cat/shards");

        Request queryDocReq = new Request("GET", "/foo/bar/1?pretty");

        RestClient masterClient = this.clients.get(this.masterServer);
        RestClient primaryShardClient = null;
        RestClient replicaShardClient = null;

        String primaryShardServerName = null;
        String replicaShardServerName = null;

        // 创建index和一个document
        doRequest(masterClient, createIndexReq);
        doRequest(masterClient, createDocReq);
        Thread.sleep(5000);

        // 筛选出主分片所在server
        String shardStr = doRequest(masterClient, queryShardReq);
        String[] shardStrList = shardStr.split("\n");
        for (String shardInfo : shardStrList) {
            String[] shardInfoList = shardInfo.trim().split(" ");
            String shardNodeName = shardInfoList[shardInfoList.length - 1];
            String shardIpAddress = shardInfoList[shardInfoList.length - 2];
            String curServerName = "server" + shardNodeName.substring(shardNodeName.length() - 1);
            if (shardInfo.contains(" p ")) {
                primaryShardServerName = curServerName;
                primaryShardClient = clients.get(primaryShardServerName);
                logger.info(String.format("PrimaryShardServer: %s; Ip address: %s", primaryShardServerName, shardIpAddress));
            } else {
                replicaShardServerName = curServerName;
                replicaShardClient = clients.get(replicaShardServerName);
                logger.info(String.format("ReplicaShardServer: %s; Ip address: %s", replicaShardServerName, shardIpAddress));
            }
        }
        assert primaryShardServerName != null && primaryShardServerName.length() != 0;
        assert replicaShardServerName != null && replicaShardServerName.length() != 0;

        // 网络分区，隔离primaryShardServer和其余两个node
        logger.info("--> partitioning node with primary shard from rest of cluster");
        NetPart netPart = NetPart.partitions(primaryShardServerName, this.masterServer + "," + replicaShardServerName).build();
        runner.runtime().networkPartition(netPart);
        Thread.sleep(10000);

        // 在和master相连的replicaShardServer上更新文档，并查询更新后的值
        logger.info("--> index a document into previous replica shard (that is now primary)");
        doRequest(replicaShardClient, updateDocReq);
        doRequest(replicaShardClient, queryDocReq);

        // 关闭replicaShardServer，之后撤销网络分区
        logger.info("--> shut down node that has new acknowledged document");
        runner.runtime().killNode(replicaShardServerName);
        Thread.sleep(1000);
        runner.runtime().removeNetworkPartition(netPart);
        logger.info("--> waiting for node with old primary shard to rejoin the cluster");
        Thread.sleep(10000);

        // 检查有着过时数据的primaryShardServer是否重新被分配上了主分片
        logger.info("--> check that old primary shard get promoted to primary again");
        for (int i = 0; i < 10; i++) {
            String queryShardRes = doRequest(primaryShardClient, queryShardReq);
            if(queryShardRes.contains("p STARTED")) break;
            Thread.sleep(5000);
        }
        doRequest(masterClient, queryDocReq);

    }


    private String doRequest(RestClient restClient, Request request) throws IOException {
        Response response = restClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        HttpHost responseHost = response.getHost();
        String responseBody = EntityUtils.toString(response.getEntity());
        if (!String.valueOf(statusCode).startsWith("2")) {
            logger.error(String.format("request to %s fails with code %d", responseHost.toHostString(), statusCode));
            logger.error("\n" + responseBody);
        } else {
            logger.info(String.format("request to %s successes with code %d", responseHost.toHostString(), statusCode));
            logger.info("\n" + responseBody);
        }
        return responseBody;
    }

    private void createLowLevelRestClients() {
        this.clients = new HashMap<>();
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            this.clients.put("server" + i, RestClient.builder(new HttpHost(runner.runtime().ip("server" + i), 9200, "http")).build());
        }
    }

    private void closeAllClients() throws IOException {
        for (RestClient client : this.clients.values()) {
            client.close();
        }
    }

    private void startServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            startServer(i);
        }
        Thread.sleep(10000);
    }

    private void startServer(int serverId) throws InterruptedException {
        new Thread(() -> {
            // 该版本可在root帐户下运行
            logger.info("server" + serverId + " startServer...");
            String command = ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch -d";
            try {
                printResult(runner.runtime().runCommandInNode("server" + serverId, command));
            } catch (RuntimeEngineException e) {
                throw new RuntimeException(e);
            }
        }).start();
        Thread.sleep(1000);
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
