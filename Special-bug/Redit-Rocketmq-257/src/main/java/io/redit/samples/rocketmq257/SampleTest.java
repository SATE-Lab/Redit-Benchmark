package io.redit.samples.rocketmq257;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
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
    private static String namesrvAddr = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        namesrvAddr = runner.runtime().ip("server1") + ":9876;" + runner.runtime().ip("server2") + ":9876";
        addRocketPropFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException, MQClientException {
        logger.info("wait for Rocketmq ...");
        givePermission();
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
        checkStatus(1);
        startProducer();
        logger.info("completed !!!");
    }


    private static void givePermission() throws RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && chmod +x bin/*";
            logger.info("server" + i + " give permission ...");
            runner.runtime().runCommandInNode("server" + i, command);
        }
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void checkStatus(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && bin/mqadmin  clusterList -n " + runner.runtime().ip("server" + serverId) + ":9876";
        logger.info("server" + serverId + " checkStatus ...");
        CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResults);
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

    private static void startProducer() throws MQClientException {
        logger.info("startProducer !!!");
        //生产者组
        DefaultMQProducer producer = new DefaultMQProducer("producer_group");
        //生产者需用通过NameServer获取所有broker的路由信息，多个用分号隔开，这个跟Redis哨兵一样
//        producer.setNamesrvAddr(namesrvAddr);
        //启动
        producer.start();
        logger.info("producer start successful !!!");

        for (int i = 1; i <= 10; i++) {
            try {
                Message msg = new Message("test_topic", "TagA", "6666", ("RocketMQ Test message " + i).getBytes());
                //SendResult是发送结果的封装，包括消息状态，消息id，选择的队列等等，只要不抛异常，就代表发送成功
                SendResult sendResult = producer.send(msg);
                System.out.println("第" + i + "条send结果: " + sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        producer.shutdown();
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
            out1.write("namesrvAddr=" + namesrvAddr);
            out2.write("namesrvAddr=" + namesrvAddr);
            out3.write("namesrvAddr=" + namesrvAddr);
            out4.write("namesrvAddr=" + namesrvAddr);
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

}
