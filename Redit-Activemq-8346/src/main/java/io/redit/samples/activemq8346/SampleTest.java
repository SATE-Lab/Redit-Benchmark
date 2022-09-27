package io.redit.samples.activemq8346;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static final String TOPIC_NAME = "VirtualTopic.TEST";
    private static String ACTIVEMQ_URL1 = "tcp://";
    private static String ACTIVEMQ_URL2 = "tcp://";

    @BeforeClass
    public static void before() throws RuntimeEngineException, InterruptedException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ACTIVEMQ_URL1 += runner.runtime().ip("server1") + ":61616";
        ACTIVEMQ_URL2 += runner.runtime().ip("server2") + ":61616";
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws Exception {
        logger.info("wait for Rocketmq ...");
        startServers();
        Thread.sleep(10000);
        checkServers();
        startConsumers();
        startProducer();
        Thread.sleep(50000);
        runner.runtime().restartNode("server1", 30);
        runner.runtime().restartNode("server2", 30);
        Thread.sleep(2000);
        startServers();
        Thread.sleep(10000);
        checkServers();
        logger.info("completed !!!");
    }

    private static void startProducer() throws JMSException {
        logger.info("startProducer !!!");
        // 连接到ActiveMQ服务器
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL1);
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);// 不使用事务，使用自动签收
        // 创建主题
        Topic topic = session.createTopic(TOPIC_NAME);
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 1; i <= 10; i++) {
            TextMessage message = session.createTextMessage("ActiveMQ Test message " + i);  // 创建一个要发送的消息对象，该对象存储的是字符串
            producer.send(message);  // 生产者负责将消息对象发送出去
            System.out.println("第" + i + "条 message send finished ...");
        }
        producer.close();
        session.close();
        connection.close();
    }

    private static void startConsumers() {
        logger.info("startConsumers !!!");
        new Thread(() -> {
            try {
                // 连接到ActiveMQ服务器
                ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL2);
                Connection connection = activeMQConnectionFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);// 不使用事务，使用自动签收
                connection.start();

                // 创建主题
//                Queue topicA = session.createQueue("Consumer.A." + TOPIC_NAME);
                Queue topicA = session.createQueue(TOPIC_NAME);
                Queue topicB = session.createQueue("Consumer.B." + TOPIC_NAME);

                // 消费者A组创建订阅
                MessageConsumer consumerA1 = session.createConsumer(topicA);
                consumerA1.setMessageListener((message) -> {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("consumerA1 接收到一个消息：" + textMessage.getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });

                MessageConsumer consumerA2 = session.createConsumer(topicA);
                consumerA2.setMessageListener((message) -> {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("consumerA2 接收到一个消息：" + textMessage.getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });

                // 消费者B组创建订阅
                MessageConsumer consumerB1 = session.createConsumer(topicB);
                consumerB1.setMessageListener((message) -> {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("consumerB1 接收到一个消息：" + textMessage.getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });

                MessageConsumer consumerB2 = session.createConsumer(topicB);
                consumerB2.setMessageListener((message) -> {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        try {
                            System.out.println("consumerB2 接收到一个消息：" + textMessage.getText());
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

    private static void startServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
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

    private static void checkServers() throws InterruptedException, RuntimeEngineException {
        for (int i = 1; i <= ReditHelper.numOfServers; i++) {
            checkServer(i);
            Thread.sleep(2000);
        }
    }

    private static void checkServer(int serverId) throws RuntimeEngineException {
        logger.info("server" + serverId + " checkServer...");
        String command1 = "cd " + ReditHelper.getRocketmq1HomeDir() + " && bin/activemq status";
        String command2 = "cd " + ReditHelper.getRocketmq2HomeDir() + " && bin/activemq status";
        String command3 = "cd " + ReditHelper.getRocketmq3HomeDir() + " && bin/activemq status";
        CommandResults commandResults1 = runner.runtime().runCommandInNode("server" + serverId, command1);
        CommandResults commandResults2 = runner.runtime().runCommandInNode("server" + serverId, command2);
        CommandResults commandResults3 = runner.runtime().runCommandInNode("server" + serverId, command3);
        printResult(commandResults1);
        printResult(commandResults2);
        printResult(commandResults3);
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
