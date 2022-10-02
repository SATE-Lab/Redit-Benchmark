package io.redit.samples.benchmark.elasticsearch.twoMaster;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class SampleTest {

    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ElasticsearchYmlFile = "elasticsearch.yml";
    private static String ElasticsearchYmlConf = "discovery.seed_hosts: [";
    private final int attemptTimes = 10;
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
    public void sampleTest() throws InterruptedException, IOException, RuntimeEngineException {
        for (int i = 1; i <= attemptTimes; i++) {
            before();
            logger.info("test round: " + i);
            startServers();
            checkJps();
            refreshMasterServer();
            boolean isCurTestSuccess = testNetworkPartition();
            after();
            if(isCurTestSuccess){
                logger.info("test success, total attempts: " + i);
                return;
            }
            else {
                logger.info("retrying......");
            }
        }
        logger.info("completed !!!");
    }

    private void refreshMasterServer() throws RuntimeEngineException {
        String command = "curl localhost:9200/_cat/master";
        String commandResult = runner.runtime().runCommandInNode("server1", command).stdOut();
        assert commandResult != null && commandResult.length() != 0;
        String[] commandResList = commandResult.split("\\s");
        for (String s : commandResList) {
            if (s.startsWith("server")) {
                this.masterServer = s;
                logger.info("Current master: " + s + ". Ip address: " + runner.runtime().ip(s));
            }
        }
    }

    private boolean testNetworkPartition() throws RuntimeEngineException, InterruptedException {
        assert masterServer != null;

        // 筛选出非master的两台服务器
        ArrayList<String> serverList = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            String serverName = "server" + i;
            if (!serverName.equals(this.masterServer)) {
                serverList.add(serverName);
            }
        }

        // 随机选择出将要与master断开连接的服务器，命名为follower
        String followerName = serverList.get(new Random().nextInt(2));
        String followerIpAddr = runner.runtime().ip(followerName);

        // 断开master和follower的连接，若该follower选举自己为master，则可以复现bug，否则需要重试
        logger.info("master server: " + this.masterServer + "; Ip address: " + runner.runtime().ip(this.masterServer));
        logger.info("follower server: " + followerName + "; Ip address: " + followerIpAddr);
        logger.info("blocking network between " + this.masterServer + " and " + followerName);

        runner.runtime().runCommandInNode(this.masterServer, String.format("iptables -I INPUT -s %s -j DROP", followerIpAddr));

        boolean continueThisTestLoop; // 是否继续本轮测试（master选举有随机性，某些情况下无法触发bug）

        for (int i = 0; i < 6; i++) {
            Thread.sleep(2500);
            continueThisTestLoop = checkAllServiceStatus();
            if(!continueThisTestLoop){ // 未触发bug，开始下一轮测试
                return false;
            }
            Thread.sleep(500);
        }

        Thread.sleep(5000);

        // 成功触发bug,向master和认为自己是master的follower询问当前master ip时，得到不同答复

        printResult(runner.runtime().runCommandInNode(this.masterServer, "curl localhost:9200/_cat/master?v"));

        printResult(runner.runtime().runCommandInNode(followerName, "curl localhost:9200/_cat/master?v"));

        return true;
    }

    private boolean checkAllServiceStatus() throws RuntimeEngineException {
        String cmd = "curl  http://localhost:9200/?pretty";
        for (int i = 1; i < ReditHelper.numOfServers + 1; i++) {
            String cmdRes = runner.runtime().runCommandInNode("server" + i, cmd).stdOut();
            if(cmdRes.contains("503")){ // 若另外一个服务器，也就是直接和master相连的follower选举为了master，则会出现503的情况，据此判断测试是否触发bug
                System.out.println(cmdRes);
                return false;
            }
        }
        return true;
    }



    private void startServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            startServer(i);
            Thread.sleep(5000);
        }
    }

    private void startServer(int serverId) throws RuntimeEngineException, InterruptedException {
        // 该版本可在root帐户下运行
        logger.info("server" + serverId + " startServer...");
        String command = ReditHelper.getElasticsearchHomeDir() + "/bin/elasticsearch -d";
        printResult(runner.runtime().runCommandInNode("server" + serverId, command));
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
