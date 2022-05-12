package io.redit.samples.benckmark.distributedid;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String[] servers = new String[ReditHelper.numOfServers + 1];

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            servers[i] = runner.runtime().ip("server" + (i)) + ":" + ReditHelper.HTTP_PORT;
        }
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for distributed-id...");
        Thread.sleep(1000);
        startServers();
        Thread.sleep(5000);
        clientCurl(1);
        logger.info("completed !!!");
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getHomeDir() + "/lib && java -jar distributed-id-1.0.0.jar 1 " + serverId;
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void clientCurl(int clientId) throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "curl " + servers[i] + "/getid";
            CommandResults result = runner.runtime().runCommandInNode("client" + clientId, command);
            printResult(result);
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
