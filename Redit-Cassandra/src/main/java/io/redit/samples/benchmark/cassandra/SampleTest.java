package io.redit.samples.benchmark.cassandra;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String CassandraYamlFile = "cassandra.yaml";
    private static String seedsIp = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        seedsIp = runner.runtime().ip("server1");
        addCassandraYamlFile();
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
        for(int i = 1; i <= 2; i++){
            startServer(i);
            Thread.sleep(10000);
        }
        checkStatus(1);
        Thread.sleep(30000);
        checkNetStatus(2);
        startServer(3);
        Thread.sleep(30000);
        checkStatus(1);
        logger.info("completed !!!");
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

    private static void addCassandraYamlFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + CassandraYamlFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("listen_address")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + CassandraYamlFile);
        }
        else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++){
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + CassandraYamlFile, true));
                String seeds = "      - seeds: \"" + seedsIp + "\"\n";
                String listen_address = "listen_address: " + runner.runtime().ip("server" + i) + "\n";
                String rpc_address = "rpc_address: " + runner.runtime().ip("server" + i) + "\n";
                out.write(seeds);
                out.write(listen_address);
                out.write(rpc_address);
                out.close();
                logger.info("add config to " + CassandraYamlFile);
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
