package io.redit.samples.activemq8168;

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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ACTIVEMQ_URL1 = "tcp://";
    private static String ACTIVEMQ_URL2 = "tcp://";
    private static ActiveMQConnectionFactory cf = null;
    private static final ActiveMQQueue destination = new ActiveMQQueue("Redelivery");
    private static final String data = "hi";
    private static final long redeliveryDelayMillis = 2000;
    private static long initialRedeliveryDelayMillis = 4000;
    private static int maxBrokerRedeliveries = 2;

    @BeforeClass
    public static void before() throws RuntimeEngineException, InterruptedException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ACTIVEMQ_URL1 += runner.runtime().ip("server1") + ":61616";
        ACTIVEMQ_URL2 += runner.runtime().ip("server2") + ":61616";

        logger.info("wait for Rocketmq ...");
        startServers();
        Thread.sleep(10000);
        checkServers();
        String url = String.format("failover:(%s,%s)", ACTIVEMQ_URL1, ACTIVEMQ_URL2);
        cf = new ActiveMQConnectionFactory(url);

        logger.info("completed !!!");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

//    @Test
//    public void sampleTest() throws Exception {
//
//    }

    @Test
    public void testScheduledRedelivery() throws Exception {
        doTestScheduledRedelivery(maxBrokerRedeliveries, true);
    }

    @Test
    public void testInfiniteRedelivery() throws Exception {
        initialRedeliveryDelayMillis = redeliveryDelayMillis;
        maxBrokerRedeliveries = RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES;
        doTestScheduledRedelivery(RedeliveryPolicy.DEFAULT_MAXIMUM_REDELIVERIES + 1, false);
    }

    private static void doTestScheduledRedelivery(int maxBrokerRedeliveriesToValidate, boolean validateDLQ) throws Exception {

        sendMessage(0);
        ActiveMQConnection consumerConnection = (ActiveMQConnection) cf.createConnection();
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setInitialRedeliveryDelay(0);
        redeliveryPolicy.setMaximumRedeliveries(0);
        consumerConnection.setRedeliveryPolicy(redeliveryPolicy);
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Message message = consumer.receive(1000);
        assertNotNull("consumer got message", message);
        logger.info("consumer got: " + message);
        consumerSession.rollback();

        for (int i = 0; i < maxBrokerRedeliveriesToValidate; i++) {
            Message shouldBeNull = consumer.receive(500);
            assertNull("did not get message early: " + shouldBeNull, shouldBeNull);

            TimeUnit.SECONDS.sleep(4);

            Message brokerRedeliveryMessage = consumer.receive(1500);
            logger.info("got: " + brokerRedeliveryMessage);
            assertNotNull("got message via broker redelivery after delay", brokerRedeliveryMessage);
            assertEquals("message matches", message.getStringProperty("data"), brokerRedeliveryMessage.getStringProperty("data"));
            assertEquals("has expiryDelay specified - iteration:" + i, i == 0 ? initialRedeliveryDelayMillis : redeliveryDelayMillis,
                    brokerRedeliveryMessage.getLongProperty(RedeliveryPlugin.REDELIVERY_DELAY));

            consumerSession.rollback();
        }

        if (validateDLQ) {
            MessageConsumer dlqConsumer = consumerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
            Message dlqMessage = dlqConsumer.receive(2000);
            assertNotNull("Got message from dql", dlqMessage);
            assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));
            consumerSession.commit();
        } else {
            // consume/commit ok
            message = consumer.receive(3000);
            assertNotNull("got message", message);
            assertEquals("redeliveries accounted for", maxBrokerRedeliveriesToValidate + 2, message.getLongProperty("JMSXDeliveryCount"));
            consumerSession.commit();
        }
        consumerConnection.close();
    }

    private static void sendMessage(int timeToLive) throws Exception {
        ActiveMQConnection producerConnection = (ActiveMQConnection) cf.createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        if (timeToLive > 0) {
            producer.setTimeToLive(timeToLive);
        }
        Message message = producerSession.createMessage();
        message.setStringProperty("data", data);
        producer.send(message);
        producerConnection.close();
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
