package io.redit.samples.benchmark.jlitespider;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String rabbitmqIp = null;

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        rabbitmqIp = runner.runtime().ip("rabbitmq1");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for jlitespider...");
        logger.info("rabbitmqIp: " + rabbitmqIp);
        Thread.sleep(20000);
        startSpider(1);
        startSpider(2);
        Thread.sleep(5000);
        startLighter(1);
        Thread.sleep(5000);
        startLighter(2);
        Thread.sleep(10000);
        logger.info("completed !!!");
    }

    private static void startSpider(int spiderId) {
        String command = "cd " + ReditHelper.getHomeDir() + " && chmod +x bin/*.sh &&  bin/run_spider.sh " + rabbitmqIp;
        logger.info("Spider" + spiderId + " command: " + command);
        logger.info("Spider" + spiderId + " startSpider...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("spider" + spiderId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startLighter(int lighterId) {
        String command = "cd " + ReditHelper.getHomeDir() + " && chmod +x bin/*.sh &&  bin/run_lighter.sh " + rabbitmqIp;
        logger.info("Lighter" + lighterId + " command: " + command);
        logger.info("Lighter" + lighterId + " startLighter...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("lighter" + lighterId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

}
