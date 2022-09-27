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
import java.util.concurrent.atomic.AtomicBoolean;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ACTIVEMQ_URL1 = "tcp://";
    private static String ACTIVEMQ_URL2 = "tcp://";

    @BeforeClass
    public static void before() throws RuntimeEngineException {
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
        testPrefetchZeroConsumerReconnectionAfterFailoverWithMessageSent(327, null);
        logger.info("completed !!!");
    }

    private static void testPrefetchZeroConsumerReconnectionAfterFailoverWithMessageSent(int messagesToSend, Integer biggerCacheSize) throws Exception {

        String url = String.format("failover:(%s,%s)?jms.prefetchPolicy.all=0", ACTIVEMQ_URL1, ACTIVEMQ_URL2);
        if (biggerCacheSize != null) {
            url += "&maxCacheSize=" + biggerCacheSize;
        }
        ConnectionFactory factory = new ActiveMQConnectionFactory(url);
        final Connection connection = factory.createConnection();
        connection.start();
        AtomicBoolean messageReceived = new AtomicBoolean(false);

        Thread consumerThread = new Thread(() -> {
            try {
                receiveMessageOnConnection(connection, "mainQueue");
                messageReceived.set(true);
            } catch (JMSException e) {
                if (e.getCause() instanceof InterruptedException) {
                    System.out.println("Interrupted because receive blocked");
                } else {
                    e.printStackTrace();
                }
            }
        });
        consumerThread.start();

        for (int i = 0; i < messagesToSend; i++) {
            // we don't actually need to send / receive a message here, just opening a consumer that does a MessagePull
            // is sufficient (receive with a timeout or receive with timeout of zero)
            sendMessageOnConnection(connection, "otherQueue", "testing " + i);
            receiveMessageOnConnection(connection, "otherQueue");
        }

        runner.runtime().restartNode("server1", 30);
        runner.runtime().restartNode("server2", 30);
        Thread.sleep(5000);
        startServers();
        Thread.sleep(10000);
        checkServers();

        sendMessageOnConnection(connection,"mainQueue", "Main Message");

        consumerThread.join(5000);

        if (messageReceived.get()) {
            System.out.println("messageReceived.get(): " + true);
        } else {
            System.out.println("messageReceived.get(): " + false);
            consumerThread.interrupt();
        }
        connection.stop();
        connection.close();
    }

    private static void receiveMessageOnConnection(Connection c, String queueName) throws JMSException {
        Session session = c.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.receive(0);
        session.commit();
        consumer.close();
        session.close();
    }

    private static void sendMessageOnConnection(Connection c, String queueName, String txt) throws JMSException {
        Session session = c.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue q = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(q);
        TextMessage message = session.createTextMessage(txt);
        message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(message);
        session.commit();
        producer.close();
        session.close();
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
