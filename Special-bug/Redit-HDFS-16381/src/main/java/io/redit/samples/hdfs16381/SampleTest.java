package io.redit.samples.hdfs16381;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.List;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static DistributedFileSystem dfs = null;
    private static String filePath = "/test/myFile.txt";
    private static Path path = new Path(filePath);
    private static final int BLOCK_SIZE = 4096;
    private static int truncateLength = 500;

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
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
    public void sampleTest1() throws IOException {
        FSDataOutputStream outputStream = dfs.create(path);
        AppendTestUtil.write(outputStream, 0, BLOCK_SIZE / 2);
        outputStream.hflush();
//        outputStream.close();  If add this, test will pass.
        dfs.truncate(path, truncateLength);
    }

    @Test
    public void sampleTest2() throws IOException {
        FSDataInputStream inputStream = dfs.open(path);
        logger.info(inputStream.readUTF());
        dfs.append(path, BLOCK_SIZE / 2, null);
    }
}