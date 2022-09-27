import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jndi.JNDIReferenceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.CompositeName;
import javax.naming.InitialContext;
import javax.naming.Reference;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ACTIVEMQ_URL1 = "tcp://";
    private static final String TOPIC_NAME = "test-topic";

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ACTIVEMQ_URL1 += runner.runtime().ip("server1") + ":61616";
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
        testActiveMQNoLocal();
        Thread.sleep(5000);
        testActiveMQNoLocal();
        logger.info("completed !!!");
    }

    private static void testActiveMQNoLocal() throws JMSException, InterruptedException {
        logger.info("testActiveMQNoLocal ...");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(ACTIVEMQ_URL1);
        connectionFactory.setClientID("test-client");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session incomingMessagesSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = incomingMessagesSession.createTopic(TOPIC_NAME);
        TopicSubscriber consumer = incomingMessagesSession.createDurableSubscriber(topic, "test-subscription", null, true);
        consumer.setMessageListener(message -> {
            try {
                System.out.println("incoming message: " + message.getJMSMessageID() + "; body: " + ((TextMessage) message).getText());
                message.acknowledge();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        Session outgoingMessagesSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = outgoingMessagesSession.createTopic(TOPIC_NAME);
        MessageProducer producer = outgoingMessagesSession.createProducer(destination);
        TextMessage textMessage = outgoingMessagesSession.createTextMessage("test-message");
        producer.send(textMessage);
        producer.close();
        System.out.println("message sent:     " + textMessage.getJMSMessageID() + "; body: " + textMessage.getText());
        outgoingMessagesSession.close();

        Thread.sleep(3000);
        consumer.close();
        incomingMessagesSession.close();
        connection.close();
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
