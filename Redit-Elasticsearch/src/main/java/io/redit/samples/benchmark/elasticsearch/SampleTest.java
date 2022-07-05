package io.redit.samples.benchmark.elasticsearch;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
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
    private static RestHighLevelClient client;

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
        Thread.sleep(5000);
        startServers();
        Thread.sleep(60000);
        checkJps();

        getEsRestClient();
        Thread.sleep(2000);
        createIndex();
        logger.info("completed !!!");
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "useradd test && runuser -l test -c '" + ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch'";
        logger.info("server" + serverId + " startServer...");
        logger.info(command);
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void getEsRestClient(){
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(runner.runtime().ip("server1"), 9200, "http")));
    }

    private static void createIndex() throws IOException {
        // 创建索引 - 请求对象
        CreateIndexRequest request1 = new CreateIndexRequest("user");
        // 发送请求，获取响应
        CreateIndexResponse response1 = client.indices().create(request1, RequestOptions.DEFAULT);
        boolean acknowledged = response1.isAcknowledged();
        // 响应状态
        System.out.println("操作状态 = " + acknowledged);

        // 查询索引 - 请求对象
        GetIndexRequest request2 = new GetIndexRequest("user");
        // 发送请求，获取响应
        GetIndexResponse response2 = client.indices().get(request2, RequestOptions.DEFAULT);
        System.out.println("aliases:" + response2.getAliases());
        System.out.println("mappings:" + response2.getMappings());
        System.out.println("settings:" + response2.getSettings());
    }

    private static void getYmlConf() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            ElasticsearchYmlConf += "\"" + runner.runtime().ip("server" + (i)) + "\"";
            if (i < ReditHelper.numOfServers){
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
            if (str.startsWith("discovery.seed_hosts")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + ElasticsearchYmlFile);
        }
        else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++){
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + ElasticsearchYmlFile, true));
                out.write(ElasticsearchYmlConf);
                out.close();
                logger.info("add config to " + ElasticsearchYmlFile + ":\n" + ElasticsearchYmlConf);
            }
        }
    }

    private void checkJps() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
        }
    }

    private static void printResult(CommandResults commandResults){
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null){
            logger.info(commandResults.stdOut());
        }else {
            logger.warn(commandResults.stdErr());
        }
    }

}
