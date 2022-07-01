package io.redit.samples.benchmark.rocketmq;

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
    private static String broker_a = "broker-a.properties";
    private static String broker_a_s = "broker-a-s.properties";
    private static String broker_b = "broker-b.properties";
    private static String broker_b_s = "broker-b-s.properties";
    private static String namesrvAddr = "namesrvAddr=";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        namesrvAddr += runner.runtime().ip("server1") + ":9876;" + runner.runtime().ip("server2") + ":9876";
        addRocketPropFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for Rocketmq ...");
        startServers();
        Thread.sleep(10000);
        startBroker(1, "a");
        Thread.sleep(10000);
        startBroker(2, "a-s");
        Thread.sleep(10000);
        startBroker(2, "b");
        Thread.sleep(10000);
        startBroker(1, "b-s");
        Thread.sleep(10000);
        checkJps();
        logger.info("completed !!!");
    }


    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && bin/mqnamesrv > ./logs/mqnamesrv.log 2>&1";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startBroker(int serverId, String broker_id) {
        String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && bin/mqbroker -c ./conf/2m-2s-async/broker-" + broker_id + ".properties > ./logs/broker-" + broker_id + ".log 2>&1";
        logger.info("server" + serverId + " startBroker, broker_id: " + broker_id);
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void addRocketPropFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/" + broker_a));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("namesrvAddr=")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config !!!");
        }
        else {
            BufferedWriter out1 = new BufferedWriter(new FileWriter("conf/" + broker_a, true));
            BufferedWriter out2 = new BufferedWriter(new FileWriter("conf/" + broker_a_s, true));
            BufferedWriter out3 = new BufferedWriter(new FileWriter("conf/" + broker_b, true));
            BufferedWriter out4 = new BufferedWriter(new FileWriter("conf/" + broker_b_s, true));
            out1.write(namesrvAddr);
            out2.write(namesrvAddr);
            out3.write(namesrvAddr);
            out4.write(namesrvAddr);
            out1.close();
            out2.close();
            out3.close();
            out4.close();
            logger.info("add config !!!" );
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

    private void checkJps() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
        }
    }
}
