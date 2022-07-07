package io.redit.samples.benchmark.activemq;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.*;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static final String DESTINATION_NAME = "test_queue";
    private static final String ACTIVEMQ_URL1 = "tcp://10.3.0.3:61616";
    private static final String ACTIVEMQ_URL2 = "tcp://10.3.0.2:61616";

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
    public void sampleTest() throws InterruptedException, RuntimeEngineException, JMSException {
        logger.info("wait for Rocketmq ...");
        startServers();
        Thread.sleep(50000);
        checkJps();
        startConsumer();
        startProducer();
        Thread.sleep(30000);
        logger.info("completed !!!");
    }


    private static void startServers() throws InterruptedException, RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(2000);
        }
    }

    private static void startServer(int serverId) throws RuntimeEngineException {
        logger.info("server" + serverId + " startServer...");
        String command1 = "cd " + ReditHelper.getRocketmq1HomeDir() + " && bin/activemq start";
        String command2 = "cd " + ReditHelper.getRocketmq2HomeDir() + " && bin/activemq start";
        String command3 = "cd " + ReditHelper.getRocketmq3HomeDir() + " && bin/activemq start";
        runner.runtime().runCommandInNode("server" + serverId, command1);
        runner.runtime().runCommandInNode("server" + serverId, command2);
        runner.runtime().runCommandInNode("server" + serverId, command3);
    }

    private static void startProducer() throws JMSException {
        logger.info("startProducer !!!");
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL1);
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start(); // 开启连接
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);// 不使用事务，使用自动签收
        Queue queue = session.createQueue(DESTINATION_NAME);  // 创建一个消息队列出来，也就是目的地
        MessageProducer producer = session.createProducer(queue);  // 把目的地放进生产者里面
        for (int i = 1; i <= 10; i++) {
            TextMessage message = session.createTextMessage("ActiveMQ Test message " + i);  // 创建一个要发送的消息对象，该对象存储的是字符串
            producer.send(message);  // 生产者负责将消息对象发送出去
            System.out.println("第" + i + "条 message send finished ...");
        }
        producer.close();
        session.close();
        connection.close();
    }

    private static void startConsumer() {
        logger.info("startConsumer !!!");
        new Thread(() -> {
            try {
                ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL2);
                Connection connection = activeMQConnectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);// 不使用事务，使用自动签收
                connection.start(); // 开启连接
                MessageConsumer consumer = session.createConsumer(session.createQueue(DESTINATION_NAME));
                consumer.setMessageListener((message) -> {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("MessageListener 接收到一个消息：" + textMessage.getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });
                System.in.read();
            } catch (JMSException | IOException e) {
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

    private void checkJps() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
        }
    }
}
