package io.redit.samples.benchmark.mapreduce;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static DistributedFileSystem dfs = null;
    private static String coordinatorIp = null;
    private static String workDir = System.getProperty("user.dir");
    private static String input_folderPath = workDir + "/src/main/java/files";
    private static String hdfsInputFolderPath = "/input";
    private static String hdfsOutputFolderPath = "/output";
    private static String hdfsTempFolderPath = "/output/temp";
    private static String mrJarPath = ReditHelper.getHadoopHomeDir() + "/lib";
    private static String jarName = "MapReduce-1.0.0.jar";

    @BeforeClass
    public static void before() throws RuntimeEngineException, ParserConfigurationException, IOException, SAXException, TransformerException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ReditHelper.waitActive();
        ReditHelper.transitionToActive(1, runner);
        ReditHelper.checkNNs(runner);
        dfs = ReditHelper.getDFS(runner);
        createAllDir();
        writeAllFileToHDFS();
        coordinatorIp = runner.runtime().ip("nn1");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException, IOException {

        startCoordinator("nn1");
        startWorker("nn2");
        startWorker("nn3");

        logger.info("wait for mapreduce ...");
        Thread.sleep(20000);
        CommandResults res = runner.runtime().runCommandInNode("nn1", ReditHelper.getHadoopHomeDir() + "/bin/hadoop fs -ls /output/");
        printResult(res);

        logger.info("mapreduce completed !!!");
    }

    public static void startCoordinator(String NodeName) {
        new Thread(() -> {
            try {
                CommandResults startCoordinator = runner.runtime().runCommandInNode(NodeName, "cd " + mrJarPath + " && java -jar " + jarName);
                printResult(startCoordinator);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void startWorker(String NodeName) {
        new Thread(() -> {
            try {
                CommandResults startWorker = runner.runtime().runCommandInNode(NodeName, "cd " + mrJarPath + " && java -cp " + jarName + " mapreduce.main.MrWorker " + coordinatorIp);
                printResult(startWorker);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void createAllDir() throws IOException {
        Path inputPath = new Path(hdfsInputFolderPath);
        Path outputPath = new Path(hdfsOutputFolderPath);
        Path tempPath = new Path(hdfsTempFolderPath);
        dfs.mkdirs(inputPath);
        dfs.mkdirs(outputPath);
        dfs.mkdirs(tempPath);
    }

    private static void writeAllFileToHDFS(){
        File input_folder = new File(input_folderPath);
        File[] files = input_folder.listFiles();
        for (File file: files){
            String hdfsPath = "hdfs://mycluster" + hdfsInputFolderPath + File.separator + file.getName();
            writeHDFS(file, hdfsPath);
        }
    }

    private static void writeHDFS(File localFile, String hdfsPath){
        FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;
        try {
            Path path = new Path(hdfsPath);
            outputStream = dfs.create(path);
            fileInputStream = new FileInputStream(localFile);
            IOUtils.copyBytes(fileInputStream, outputStream,4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileInputStream != null){
                IOUtils.closeStream(fileInputStream);
            }
            if(outputStream != null){
                IOUtils.closeStream(outputStream);
            }
        }
    }

    private static void printResult(CommandResults commandResults){
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null){
            logger.info(commandResults.stdOut());
        }else {
            logger.warn(commandResults.stdErr());
        }
    }

}