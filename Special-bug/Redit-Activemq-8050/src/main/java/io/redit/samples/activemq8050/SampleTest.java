package io.redit.samples.activemq8050;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;


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
        Thread.sleep(2000);
        failoverWithExceptionProgrammaticBrokers();
        Thread.sleep(2000);
        checkServers();
        logger.info("completed !!!");
    }



    private static Message getMessage(String messageId, ActiveMQMessageConsumer consumer) throws JMSException {
        String receivedMessageId = null;
        Instant start = Instant.now();
        while (receivedMessageId == null || !Objects.equals(messageId, receivedMessageId)) {
            if (Instant.now().isAfter(start.plus(5, ChronoUnit.SECONDS))) {
                Assert.fail("timeout");
            }
            Message msg = consumer.receive(20000);
            Assert.assertNotNull("Couldn't get message", msg);
            receivedMessageId = msg.getStringProperty("my_id");
            if (!Objects.equals(messageId, receivedMessageId)) {
                logger.info("Got the wrong message. Looping.");
            } else {
                logger.info("Found message");
                return msg;
            }
        }
        return null;
    }

    private static Xid createXid() throws IOException {
        final AtomicLong txGenerator = new AtomicLong(System.currentTimeMillis());

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(txGenerator.incrementAndGet());
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            @Override
            public int getFormatId() {
                return 86;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return bs;
            }

            @Override
            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }

    // @Test
    public void failoverWithExceptionProgrammaticBrokers() throws Exception {


        XAConnection producerConnection = null;
        ActiveMQXAConnection consumerConnection = null;
        try {

            String queueName = "MY_QUEUE";

            String url = String.format("failover:(%s,%s)", ACTIVEMQ_URL1, ACTIVEMQ_URL2);
            ActiveMQXAConnectionFactory firstFactory = new ActiveMQXAConnectionFactory(url);
            producerConnection = firstFactory.createXAConnection();
            producerConnection.setClientID("PRODUCER");
            producerConnection.start();
            XASession producerSession = producerConnection.createXASession();
            Queue producerDestination = producerSession.createQueue(queueName);
            Xid xid = createXid();
            producerSession.getXAResource().start(xid, XAResource.TMNOFLAGS);
            String messageId = UUID.randomUUID().toString();
            MessageProducer producer = producerSession.createProducer(producerDestination);
            TextMessage sendMessage = producerSession.createTextMessage("Test message");
            sendMessage.setStringProperty("my_id", messageId);
            producer.send(sendMessage);
            producerSession.getXAResource().end(xid, XAResource.TMSUCCESS);
            producerSession.getXAResource().prepare(xid);
            producerSession.getXAResource().commit(xid, false);

            consumerConnection = (ActiveMQXAConnection) firstFactory.createXAConnection();
            consumerConnection.setClientID("CONSUMER");
            consumerConnection.start();
            XASession consumerSession = consumerConnection.createXASession();
            Queue consumerDestination = consumerSession.createQueue(queueName);
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) consumerSession.createConsumer(consumerDestination);

            Xid consumerXid = createXid();
            consumerSession.getXAResource().start(consumerXid, XAResource.TMNOFLAGS);
            Message message = getMessage(messageId, consumer);
            consumerSession.getXAResource().end(consumerXid, XAResource.TMSUCCESS);
            consumerSession.getXAResource().prepare(consumerXid);

            logger.info("Simulating dropped connection");
            FailoverTransport transport = consumerConnection.getTransport().narrow(FailoverTransport.class);
            URI currentTransport = transport.getConnectedTransportURI();
            transport.handleTransportFailure(new IOException("Fake fail"));
            await().atMost(Duration.ofSeconds(10)).until(() -> !Objects.equals(currentTransport, transport.getConnectedTransportURI()) && transport.isConnected());
            Assert.assertTrue(transport.isConnected());
            Assert.assertNotEquals(currentTransport, transport.getConnectedTransportURI());
            message.acknowledge();
            consumerSession.getXAResource().commit(consumerXid, false);
        } catch (XAException e) {
            if (e.errorCode == -4) {
                logger.info("Recreated error successfully");
            } else {
                logger.error("Got XAException " + e.errorCode, e);
                Assert.fail();
            }
        } finally {
            producerConnection.close();
            consumerConnection.close();
//            broker1.getNetworkConnectors().get(0).stop();
//            broker2.getNetworkConnectors().get(0).stop();
//            await().atMost(Duration.ofSeconds(5)).until(() -> broker1.getNetworkConnectors().get(0).isStopped() && broker2.getNetworkConnectors().get(0).isStopped());
//            broker1.stop();
//            broker2.stop();
        }
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
