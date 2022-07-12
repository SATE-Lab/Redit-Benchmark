package io.redit.samples.benchmark.zookeeper;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String confFile = "conf/zoo.cfg";
    private static String serverConf = "";
    private static String connectionStr = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            serverConf += "server." + i + "=" + runner.runtime().ip("server" + (i)) + ":2888:3888\n";
        }
        connectionStr = runner.runtime().ip("server1") + ":2181," + runner.runtime().ip("server2") + ":2181," + runner.runtime().ip("server3") + ":2181";
        addConfFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws Exception {
        logger.info("wait for zookeeper...");
        startServers();
        Thread.sleep(20000);
        checkServersStatus();
        Thread.sleep(2000);
        createTmpZnode();
        logger.info("completed !!!");
    }

    private static void startServers() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
        }
    }

    private static void checkServersStatus() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "cd " + ReditHelper.getHomeDir() + " && bin/zkServer.sh status";
            logger.info("server" + i + " checkStatus...");
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, command);
            printResult(commandResults);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getHomeDir() + " && bin/zkServer.sh start";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void createTmpZnode() throws Exception {
        //1:定制一个重试策略
        /*
            param1: 重试的间隔时间
            param2:重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        //2:获取一个客户端对象
        /*
           param1:要连接的Zookeeper服务器列表
           param2:会话的超时时间
           param3:链接超时时间
           param4:重试策略
         */
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3:启动客户端
        client.start();
        //4:创建临时节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello", "zookeeper".getBytes());
        Thread.sleep(5000);
        //5:获取节点数据
        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));
        //6:关闭客户端
        client.close();
    }

    private static void addConfFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(confFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("server")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + confFile);
        }
        else {
            BufferedWriter out = new BufferedWriter(new FileWriter(confFile, true));
            out.write(serverConf);
            out.close();
            logger.info("add config to " + confFile + ":\n" + serverConf);
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
