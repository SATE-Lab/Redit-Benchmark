package io.redit.samples.benchmark.mapreduce;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static DistributedFileSystem dfs = null;

    @BeforeClass
    public static void before() throws RuntimeEngineException, ParserConfigurationException, IOException, SAXException, TransformerException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ReditHelper.waitActive();
        logger.info("The cluster is UP!");
        ReditHelper.transitionToActive(1, runner);
        ReditHelper.checkNNs(runner);
        dfs = ReditHelper.getDFS(runner);
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws IOException, InterruptedException {
        logger.info("NN1 ip: " + runner.runtime().ip("nn1"));
        dfs.mkdirs(new Path("/output"));
        Thread.sleep(1000);

    }



}