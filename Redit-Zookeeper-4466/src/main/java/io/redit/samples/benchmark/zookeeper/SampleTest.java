package io.redit.samples.benchmark.zookeeper;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT;
import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;
import static org.junit.Assert.*;


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
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
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
        testPathOverlapWithStandardWatcher();
        testPathOverlapWithPersistentWatcher();
        logger.info("completed !!!");
    }

    private static void startServers() {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            startServer(i);
        }
    }

    private static void checkServersStatus() throws RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
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

    public void testPathOverlapWithStandardWatcher() throws RuntimeException {
        BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        Watcher persistentWatcher = events::add;
        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(connectionStr, 4000, watchedEvent -> countDownLatch.countDown());
            countDownLatch.await();
            zk.addWatch("/a", persistentWatcher, PERSISTENT_RECURSIVE);
            zk.exists("/a", event -> {});

            zk.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.delete("/a/b", -1);
            zk.delete("/a", -1);

            assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a");
            assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a");
            assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testPathOverlapWithPersistentWatcher() throws RuntimeException {
        BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        Watcher persistentWatcher = events::add;
        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(connectionStr, 4000, watchedEvent -> countDownLatch.countDown());
            countDownLatch.await();

            zk.addWatch("/a", persistentWatcher, PERSISTENT_RECURSIVE);
            zk.addWatch("/a/b", event -> {}, PERSISTENT);

            zk.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/a/b/c", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.delete("/a/b/c", -1);
            zk.delete("/a/b", -1);
            zk.delete("/a", -1);

            assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a");
            assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a/b/c");
            assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a/b/c");
            assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a");
            assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertEvent(BlockingQueue<WatchedEvent> events, Watcher.Event.EventType eventType, String path)
            throws InterruptedException {
        WatchedEvent event = events.poll(5, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(eventType, event.getType());
        assertEquals(path, event.getPath());
    }

    private static void addConfFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(confFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("server")) {
                addConf = true;
            }
        }
        in.close();
        if (addConf) {
            logger.info("already add config in " + confFile);
        } else {
            BufferedWriter out = new BufferedWriter(new FileWriter(confFile, true));
            out.write(serverConf);
            out.close();
            logger.info("add config to " + confFile + ":\n" + serverConf);
        }
    }

    private static void printResult(CommandResults commandResults) {
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null) {
            logger.info(commandResults.stdOut());
        } else {
            logger.warn(commandResults.stdErr());
        }
    }

}
