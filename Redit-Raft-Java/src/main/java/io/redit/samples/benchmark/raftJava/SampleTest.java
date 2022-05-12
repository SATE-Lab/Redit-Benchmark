package io.redit.samples.benchmark.raftJava;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String[] servers = new String[ReditHelper.numOfServers + 1];
    private static String serverCluster = "";
    private static String storageAddress = "list://";
    private static String key = "hello";
    private static String value = "zmb";

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        getClusterInfo();
        logger.info("servers: " + Arrays.toString(servers));
        logger.info("serverCluster: " + serverCluster);
        logger.info("storageAddress: " + storageAddress);
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException {
        logger.info("wait for raft-java...");
        startServers();
        Thread.sleep(10000);
        clientWrite(1);
        Thread.sleep(5000);
        clientRead(2);
        Thread.sleep(5000);
        logger.info("completed !!!");
    }

    private static void clientWrite(int clientId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && chmod +x bin/*.sh &&  bin/run_client.sh \"" + storageAddress + "\" " + key + " " + value;
        logger.info(command);
        logger.info("client" + clientId + " clientWrite...");
        new Thread(() -> {
            try {
                CommandResults result = runner.runtime().runCommandInNode("client" + clientId, command);
                printResult(result);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void clientRead(int clientId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && chmod +x bin/*.sh && bin/run_client.sh \"" + storageAddress + "\" " + key;
        logger.info(command);
        logger.info("client" + clientId + " clientRead...");
        new Thread(() -> {
            try {
                CommandResults result = runner.runtime().runCommandInNode("client" + clientId, command);
                printResult(result);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= io.redit.samples.benchmark.test.ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && bin/run_server.sh ./data \"" + serverCluster + "\" \"" + servers[serverId] + "\" &";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, "chmod +x " + ReditHelper.getRaftHomeDir() + "/bin/*.sh");
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void getClusterInfo(){
        String nodesInfo, nodesStorage;
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            nodesInfo = runner.runtime().ip("server" + (i)) + ":" + ReditHelper.RPC_PORT + ":" + (i);
            nodesStorage = runner.runtime().ip("server" + (i)) + ":" + ReditHelper.RPC_PORT;
            servers[i] = nodesInfo;
            serverCluster += nodesInfo;
            storageAddress += nodesStorage;
            if (i != ReditHelper.numOfServers){
                serverCluster += ",";
                storageAddress += ",";
            }
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
