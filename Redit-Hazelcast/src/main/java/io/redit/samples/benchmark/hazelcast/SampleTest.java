package io.redit.samples.benchmark.hazelcast;

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

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException {
        logger.info("wait for raft-java...");
        startServers();
        Thread.sleep(20000);
        checkServers();
        Thread.sleep(5000);
        logger.info("completed !!!");
    }

    private void checkServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            checkServer(i);
            Thread.sleep(1000);
        }
    }

    private void checkServer(int serverId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && bin/hz-cli --config config/hazelcast-client.xml cluster ";
        logger.info("server" + serverId + " checkServer...");
        logger.info(command);
        new Thread(() -> {
            try {
                CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
                printResult(commandResults);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && bin/hz-start ";
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

    private static void printResult(CommandResults commandResults){
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null){
            logger.info(commandResults.stdOut());
        }else {
            logger.warn(commandResults.stdErr());
        }
    }
}
