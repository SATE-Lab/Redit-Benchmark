package io.redit.samples.benchmark.hadoop.mapreduce;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.hadoop.fs.FSDataInputStream;
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
    private static String workDir = System.getProperty("user.dir");
    private static String input_folderPath = workDir + "/src/main/java/files";
    private static String hdfsInputFolderPath = "/input";
    private static String hdfsInputFilePath = "/input/pg-grimm.txt";
    private static String hdfsOutputFolderPath = "/output";
    private static String jarName = "hadoop-mapreduce-examples-3.3.1.jar";

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
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException, IOException {

        startMrJob("nn1");
        logger.info("wait for mapreduce ...");
        Thread.sleep(20000);
        checkMrJob("nn2");
        logger.info("mapreduce completed !!!");
    }

    private static void startMrJob(String NodeName) {
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode(NodeName, "cd " + ReditHelper.getHadoopHomeDir()  + " && bin/hadoop jar ./share/hadoop/mapreduce/" + jarName + " wordcount " + hdfsInputFilePath + " " + hdfsOutputFolderPath);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void checkMrJob(String NodeName) throws RuntimeEngineException, IOException {
        CommandResults res1 = runner.runtime().runCommandInNode(NodeName, "cd " + ReditHelper.getHadoopHomeDir()  + " && bin/hadoop fs -ls " + hdfsOutputFolderPath);
        printResult(res1);
        Path path = new Path(hdfsOutputFolderPath + "/part-r-00000");
        FSDataInputStream inputStream = dfs.open(path);
        int ch = inputStream.read();
        while (ch != -1){
            System.out.print((char)ch);
             ch = inputStream.read();
        }
    }

    private static void createAllDir() throws IOException {
        Path inputPath = new Path(hdfsInputFolderPath);
        dfs.mkdirs(inputPath);
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