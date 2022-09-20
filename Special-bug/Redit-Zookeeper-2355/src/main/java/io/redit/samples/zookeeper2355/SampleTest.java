package io.redit.samples.zookeeper2355;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import io.redit.execution.NetPart;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNull;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String confFile = "conf/zoo.cfg";
    private static String serverConf = "";
    private static String connectionStr = "";
    private static int follower = 0;
    private static int leader = 0;

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
        logger.info("follower: server" + follower);

        test2355();
        logger.info("completed !!!");
    }

    public void test2355() throws InterruptedException, RuntimeEngineException, IOException, KeeperException {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(connectionStr, 4000, watchedEvent -> countDownLatch.countDown());
        countDownLatch.await();

        // 1: create ephemeral node
        String nodePath = "/e1";
        zk.create(nodePath, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(2000);
        logger.info("create ephemeral node on " + nodePath);

        // 2: inject network problem in one of the follower
        List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
        list.remove(follower - 1);
        int random = (int) (Math.random() * 2);
        String otherServer = "server" + list.get(random);
        logger.info("otherServer: " + otherServer);
        NetPart netPart = NetPart.partitions(otherServer, "server" + follower).build();
        runner.runtime().networkPartition(netPart);

        // 3: close the session so that ephemeral node is deleted
        Thread.sleep(10000);
        zk.close();
        Thread.sleep(2000);
        logger.info("session closed");

        // remove the error
        runner.runtime().removeNetworkPartition(netPart);
        Thread.sleep(2000);
        logger.info("network partition stopped");


        CountDownLatch countDownLatch2 = new CountDownLatch(1);
        zk = new ZooKeeper(runner.runtime().ip("server" + leader) + ":2181",4000, watchedEvent -> countDownLatch2.countDown());
        Stat exists = zk.exists(nodePath, false);
        logger.info("Node have not been deleted from leader: " + exists.toString());
//        assertNull("Node must have been deleted from leader", exists);

        CountDownLatch countDownLatch3 = new CountDownLatch(1);
        ZooKeeper followerZK = new ZooKeeper(runner.runtime().ip("server" + follower) + ":2181", 4000, watchedEvent -> countDownLatch3.countDown());
        Stat nodeAtFollower = followerZK.exists(nodePath, false);

        // Problem 1: Follower had one extra ephemeral node /e1
        logger.info("Follower had one extra ephemeral node /e1: " + nodeAtFollower.toString());
//        assertNull("ephemeral node must not exist", nodeAtFollower);

        // create the node with another session
        zk.create(nodePath, "2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // close the session and newly created ephemeral node should be deleted
        Thread.sleep(5000);
        zk.close();
        Thread.sleep(5000);

        nodeAtFollower = followerZK.exists(nodePath, false);

        // Problem 2: Before fix, after session close the ephemeral node
        // was not getting deleted. But now after the fix after session close
        // ephemeral node is getting deleted.
        assertNull("After session close ephemeral node must be deleted", nodeAtFollower);
        followerZK.close();
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
            if (commandResults.stdOut().indexOf("follower") != -1 && follower == 0){
                follower = i;
            }else if (commandResults.stdOut().indexOf("leader") != -1){
                leader = i;
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

    private static void printResult(CommandResults commandResults){
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null){
            logger.info(commandResults.stdOut());
        }else {
            logger.warn(commandResults.stdErr());
        }
    }

}
