package io.redit.samples.cassandra15297;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static final String test_data_file = "/opt/cassandra/data_file_directories/system_schema/aggregates-924c55872e3a345bb10c12f37c1ba895";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        makeDirs();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException {
        logger.info("wait for Cassandra ...");
        startServer(1);
        Thread.sleep(10000);
        startServer(2);
        Thread.sleep(30000);
        checkNetStatus(2);
        checkStatus(1);

        Thread.sleep(3000);
        snapshotTest1(1);
        snapshotTest2(2);
        logger.info("completed !!!");
    }

    private static void snapshotTest1 (int serverId) throws RuntimeEngineException {
        logger.info("snapshotTest1 test snapshot \"p/s\" ...");
        String command1 = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool snapshot -t \"p/s\"";
        String command2 = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool listsnapshots";
        String command3 = "cd " + test_data_file + "/snapshots/p/s" + " && ls -l";

        CommandResults commandResult1 = runner.runtime().runCommandInNode("server" + serverId, command1);
        CommandResults commandResult2 = runner.runtime().runCommandInNode("server" + serverId, command2);
        CommandResults commandResult3 = runner.runtime().runCommandInNode("server" + serverId, command3);
        printResult(commandResult1);
        printResult(commandResult2);
        printResult(commandResult3);
    }

    private static void snapshotTest2 (int serverId) throws RuntimeEngineException {
        logger.info("/n snapshotTest2 test snapshot \"/\" ...");
        String command1 = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool snapshot -t \"/\"";
        String command2 = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool listsnapshots";
        String command3 = "cd " + test_data_file + "/snapshots" + " && ls -l";

        CommandResults commandResult1 = runner.runtime().runCommandInNode("server" + serverId, command1);
        CommandResults commandResult2 = runner.runtime().runCommandInNode("server" + serverId, command2);
        CommandResults commandResult3 = runner.runtime().runCommandInNode("server" + serverId, command3);
        printResult(commandResult1);
        printResult(commandResult2);
        printResult(commandResult3);
    }

    private static void makeDirs() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "mkdir /opt/cassandra && cd /opt/cassandra && mkdir data_file_directories commitlog_directory saved_caches_directory hints_directory";
            runner.runtime().runCommandInNode("server" + i, command);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/cassandra -R ";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void checkNetStatus(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool netstats ";
        CommandResults commandResult = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResult);
    }

    private static void checkStatus(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool status ";
        CommandResults commandResult = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResult);
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
