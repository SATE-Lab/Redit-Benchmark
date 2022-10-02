package io.redit.samples.benchmark.elasticsearch.minimumMasterNodes;

import com.alibaba.fastjson.JSONObject;
import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ElasticsearchYmlFile = "elasticsearch.yml";
    private static String ElasticsearchYmlConf = "discovery.seed_hosts: [";
    private static RestHighLevelClient client;
    private static final String INDEX_NAME = "test";

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
        Thread.sleep(2000);
        startServers();

        Thread.sleep(10000);
        checkJps();
        runCommands();

        getEsRestClient();
        Thread.sleep(2000);
        addIndex();
        searchById("0");
        searchList("鉴定药敏分析仪");
        logger.info("completed !!!");
    }

    private void runCommands() throws RuntimeEngineException {
        String c1 = "curl -X GET http://localhost:9200?pretty";
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            printResult(runner.runtime().runCommandInNode("server" + i, c1));
        }
        System.out.println("--------------");
    }

    private static void startServers() throws InterruptedException, RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(2000);
            checkJps();
        }
    }

    private static void startServer(int serverId) throws RuntimeEngineException, InterruptedException {
//        String command = "useradd test && runuser -l test -c '" + ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch'";
//        logger.info("server" + serverId + " startServer...");
//        new Thread(() -> {
//            try {
//                runner.runtime().runCommandInNode("server" + serverId, command);
//            } catch (RuntimeEngineException e) {
//                e.printStackTrace();
//            }
//        }).start();

        logger.info("server" + serverId + " startServer...");
        printResult(runner.runtime().runCommandInNode("server"+serverId, "useradd test"));
        printResult(runner.runtime().runCommandInNode("server"+serverId, "chown -R test /elasticsearch"));
        Thread.sleep(1000);
        String command = "runuser -l test -c '" + ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch -d'";
        printResult(runner.runtime().runCommandInNode("server" + serverId, command));
        Thread.sleep(5000);
        printResult(runner.runtime().runCommandInNode("server"+serverId, "curl -X GET http://localhost:9200/?pretty"));
    }

    private static void getEsRestClient(){
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(runner.runtime().ip("server1"), 9200, "http")));
        System.out.println(client);
    }

    public static void addIndex() throws IOException {
        /*初始化 查询请求操作，指定操作 index 名称*/
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        /*实例化数据*/
        Tender tender = new Tender("鉴定药敏分析仪","四川国际招标有限责任公司","招标结果","5");
        String json = JSONObject.toJSONString(tender);
        /*设置 类型，7.x 不用设置，默认是 _doc*/
        indexRequest.type("data");
        /*数据转换成  json 添加进去*/
        indexRequest.source(json, XContentType.JSON);
        /*添加索引 */
        client.index(indexRequest, RequestOptions.DEFAULT);
        client.close();
    }

    public static Tender searchById(String id) throws IOException {
        /*初始化 get  请求*/
        GetRequest request = new GetRequest(INDEX_NAME, "data", id);
        /*按照 ID 获取数据*/
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        Tender tender = JSONObject.parseObject(response.getSourceAsString(), Tender.class);
        client.close();
        return tender;
    }

    public static void searchList(String keyword) throws IOException {
        /*初始化查询请求*/
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        /*初始化 构建 查询 builder*/
        SearchSourceBuilder builder = new SearchSourceBuilder();
        /*初始化 多字段 查询 builder*/
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyword, "product_agency");
        /*设置 查询 多字段查询*/
        builder.query(multiMatchQueryBuilder);
        /*把 构建好的 查询 封装到 查询请求中*/
        searchRequest.source(builder);
        /*执行查询*/
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        List<Tender> collect = Arrays.stream(hits).map(hit -> JSONObject.parseObject(hit.getSourceAsString(), Tender.class)).collect(Collectors.toList());
        System.out.println(collect);
        client.close();
    }


    private static void getYmlConf() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            ElasticsearchYmlConf += "\"" + runner.runtime().ip("server" + (i)) + ":9300\"";
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

    private static void checkJps() throws RuntimeEngineException {
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
