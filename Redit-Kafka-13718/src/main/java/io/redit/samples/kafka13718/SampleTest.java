package io.redit.samples.kafka13718;

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
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String KafkaPropFile = "server.properties";
    private static String ZooCfgConf = "";
    private static String KafkaPropConf = "zookeeper.connect=";
    private static String topicName = "test";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            ZooCfgConf += "server." + i + "=" + runner.runtime().ip("server" + (i)) + ":2888:3888\n";
            KafkaPropConf += runner.runtime().ip("server" + (i)) + ":2181";
            if (i < ReditHelper.numOfServers){
                KafkaPropConf += ",";
            }
        }
        addZooCfgFile();
        addKafkaPropFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for zookeeper...");
        startZookeepers();
        Thread.sleep(10000);
        checkZookeepersStatus();

        logger.info("wait for kafka...");
        startKafkas();
        Thread.sleep(5000);
        checkJps();

        createTopic(1);
        logger.info("completed !!!");
    }

    private void createTopic(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-topics.sh --bootstrap-server " + runner.runtime().ip("server" + serverId) + ":9092 --create --topic " + topicName;
        CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResults);
        String command2 = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-topics.sh --bootstrap-server " + runner.runtime().ip("server" + serverId) + ":9092 --describe --topic " + topicName;
        CommandResults commandResults2 = runner.runtime().runCommandInNode("server" + serverId, command2);
        printResult(commandResults2);
    }

    private void startKafkas() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startKafka(i);
        }
    }

    private static void startKafka(int serverId) {
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-server-start.sh -daemon ./config/server.properties";
        logger.info("server" + serverId + " startKafka...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startZookeepers() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startZookeeper(i);
        }
    }

    private static void startZookeeper(int serverId) {
        String command = "cd " + ReditHelper.getZookeeperHomeDir() + " && bin/zkServer.sh start";
        logger.info("server" + serverId + " startZookeeper...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void checkZookeepersStatus() throws InterruptedException, RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "cd " + ReditHelper.getZookeeperHomeDir() + " && bin/zkServer.sh status";
            logger.info("server" + i + " checkStatus...");
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, command);
            printResult(commandResults);
            Thread.sleep(1000);
        }
    }

    private static void addKafkaPropFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + KafkaPropFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("zookeeper.connect=")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + KafkaPropFile);
        }
        else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++){
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + KafkaPropFile, true));
                String listenerConf = "listeners = PLAINTEXT://" + runner.runtime().ip("server" + i) + ":9092\n";
                out.write(listenerConf);
                out.write(KafkaPropConf);
                out.close();
                logger.info("add config to " + KafkaPropFile + ":\n" + KafkaPropConf);
            }
        }
    }

    private static void addZooCfgFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(ZooCfgFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("server")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + ZooCfgFile);
        }
        else {
            BufferedWriter out = new BufferedWriter(new FileWriter(ZooCfgFile, true));
            out.write(ZooCfgConf);
            out.close();
            logger.info("add config to " + ZooCfgFile + ":\n" + ZooCfgConf);
        }
    }

    private void checkJps() throws RuntimeEngineException {
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
