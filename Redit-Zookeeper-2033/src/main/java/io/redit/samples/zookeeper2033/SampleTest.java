package io.redit.samples.zookeeper2033;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String confFile = "conf/zoo.cfg";
    private static String serverConf = "";
    private static final int SESSION_TIMEOUT = 30000;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static String connectionStr = "";
    private static ZooKeeper zooKeeper;
    private static List<Integer> followers = new ArrayList<>();

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
        logger.info("followers: " + followers);
        test2033();
        logger.info("completed !!!");
    }

    private static void test2033() throws Exception {
        getConnect();
        createNode();
        Thread.sleep(5000);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            runner.runtime().restartNode("server" + i, 30);
        }
        checkJps();
        deleteFollowerFiles();

        startServersExceptFollower();
        Thread.sleep(10000);
        checkServersStatus();

        for (int follower: followers){
            startServer(follower);
        }
        Thread.sleep(20000);
        checkServersStatus();

        for (int follower: followers){
            runner.runtime().restartNode("server" + follower, 30);
        }
        for (int follower: followers){
            startServer(follower);
        }
        Thread.sleep(20000);
        checkServersStatus();
    }


    private static void getConnect() throws IOException {
        Watcher watcher = event -> {
            if(Watcher.Event.KeeperState.SyncConnected == event.getState()){
                countDownLatch.countDown();
                String msg = String.format("process info,eventType:%s,eventState:%s,eventPath:%s", event.getType(),event.getState(),event.getPath());
                logger.info(msg);
            }
        };
        zooKeeper = new ZooKeeper(connectionStr, SESSION_TIMEOUT, watcher);
        try {
            countDownLatch.await();
            logger.info("Zookeeper session establish success,sessionID = " + Long.toHexString(zooKeeper.getSessionId()));
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.debug("Zookeeper session establish fail");
        }
    }

    private static void createNode() throws KeeperException, InterruptedException {
        String inputString = "test";
        byte[] input = inputString.getBytes();
        String path = "/newepochzxidtest";
        String result = null;
        try {
            result = zooKeeper.create(path, input, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(10000);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info("create node success,result: " + result);
        zooKeeper.setData(path, input, -1);
        byte[] output = zooKeeper.getData(path, false, null);
        logger.info("zooKeeper.getData: " + output);
        zooKeeper.close();
    }

    private static void deleteFollowerFiles() throws RuntimeEngineException {
        for (int follower: followers){
            logger.info("delete follower files: server" + follower);
            String dataDir = "/zookeeper/apache-zookeeper-3.4.6-bin/zkdata";
            checkFiles(follower, dataDir);
            deleteFiles(follower, dataDir);
            checkFiles(follower, dataDir);
        }
    }

    private static void checkFiles(int serverId, String dataDir) throws RuntimeEngineException {
        String command = "ls -l " + dataDir;
        CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResults);
    }

    private static void deleteFiles(int serverId, String dataDir) throws RuntimeEngineException {
        String command = "rm -rf " + dataDir + "/*";
        CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResults);
    }

    private static void startServers() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
        }
    }

    private static void startServersExceptFollower() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            if (!followers.contains(i)) {
                startServer(i);
            }
        }
    }

    private static void checkServersStatus() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "cd " + ReditHelper.getHomeDir() + " && bin/zkServer.sh status";
            logger.info("server" + i + " checkStatus...");
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, command);
            printResult(commandResults);
            if (commandResults.stdOut().indexOf("follower") != -1 && followers.size() < ReditHelper.numOfServers - 1){
                followers.add(i);
            }
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
