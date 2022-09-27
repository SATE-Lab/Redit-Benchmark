package io.redit.samples.activemq6900;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jndi.JNDIReferenceFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.CompositeName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
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
        testTrustedPackages();
        logger.info("completed !!!");
    }

    private static void testTrustedPackages() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(ACTIVEMQ_URL1);
        cf.setTrustAllPackages(true);
        List<String> trustedPackages = Arrays.asList("java.lang", "java.util");
        cf.setTrustedPackages(trustedPackages);

        Reference reference = JNDIReferenceFactory.createReference(ActiveMQConnectionFactory.class.getName(), cf);
        JNDIReferenceFactory factory = new JNDIReferenceFactory();
        Object object = factory.getObjectInstance(
                reference,
                new CompositeName("jms/ConnectionFactory"),
                new InitialContext(),
                new Hashtable<>()
        );
        assertTrue(object instanceof ActiveMQConnectionFactory);
        ActiveMQConnectionFactory jndiCf = (ActiveMQConnectionFactory)object;
        logger.info("jndiCf.isTrustAllPackages(): " + jndiCf.isTrustAllPackages());
        List<String> jndiTrustedPackages = jndiCf.getTrustedPackages();
        logger.info("jndiTrustedPackages.size(): " + jndiTrustedPackages.size() + "  expected: 2");
        assertEquals(trustedPackages, jndiTrustedPackages);
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
