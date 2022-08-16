package io.redit.samples.benchmark.hbase;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.commons.math3.util.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.Arrays;
import java.util.List;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String HbaseSiteFile = "conf/hbase-site.xml";
    private static String RegionFile = "conf/regionservers";
    private static String ZooCfgConf = "";
    private static String HbaseSiteConf = "";
    private static String RegionConf = "";
    private static final String TABLE_NAME = "class";
    private static final String TEACHER = "teacher";
    private static final String STUDENT = "student";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        ReditHelper.waitActive();
        ReditHelper.transitionToActive(1, runner);
        ReditHelper.checkNNs(runner);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            ZooCfgConf += "server." + i + "=" + runner.runtime().ip("server" + (i)) + ":2888:3888\n";
            HbaseSiteConf += runner.runtime().ip("server" + (i)) + ":2181";
            RegionConf += runner.runtime().ip("server" + (i));
            if (i < ReditHelper.numOfServers){
                HbaseSiteConf += ",";
                RegionConf += "\n";
            }

        }
        addZooCfgFile();
        addHbaseSiteConf();
        addRegionConf();
    }


    @AfterClass
    public static void after() throws IOException {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws Exception {
        logger.info("wait for zookeeper...");
        startZookeepers();
        Thread.sleep(5000);
        checkZookeepersStatus();

        logger.info("wait for hbase...");
        startSsh();
        Thread.sleep(2000);
        startHbases();
        Thread.sleep(20000);
        checkJps();

        logger.info("wait for createTable and insertTable...");
        createZnode();
        createTable();
        insertData();
        getRow();
        getCell();
        getScanner();
        getScannerWithFilter();
        deleteColumn();
        deleteRow();
        deleteTable();
        logger.info("completed !!!");
    }

    private static void startHbases() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startHbase(i);
        }
    }

    private static void startHbase(int serverId) {
        String command = "cd " + ReditHelper.getHbaseHomeDir() + " && bin/hbase-daemon.sh start master && bin/hbase-daemon.sh start regionserver";
        logger.info("server" + serverId + " startHbase...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void createZnode() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectionStr = runner.runtime().ip("server1") + ":2181," + runner.runtime().ip("server2") + ":2181," + runner.runtime().ip("server3") + ":2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hbase/id", "hbase".getBytes());
        Thread.sleep(5000);
        client.close();
    }

    private static void createTable(){
        List<String> columnFamilies = Arrays.asList(TEACHER, STUDENT);
        boolean table = HBaseUtils.createTable(TABLE_NAME, columnFamilies);
        System.out.println("表创建结果:" + table);
    }

    public void insertData() {
        List<Pair<String, String>> pairs1 = Arrays.asList(new Pair<>("name", "Tom"),
                new Pair<>("age", "22"),
                new Pair<>("gender", "1"));
        HBaseUtils.putRow(TABLE_NAME, "rowKey1", STUDENT, pairs1);

        List<Pair<String, String>> pairs2 = Arrays.asList(new Pair<>("name", "Jack"),
                new Pair<>("age", "33"),
                new Pair<>("gender", "2"));
        HBaseUtils.putRow(TABLE_NAME, "rowKey2", STUDENT, pairs2);

        List<Pair<String, String>> pairs3 = Arrays.asList(new Pair<>("name", "Mike"),
                new Pair<>("age", "44"),
                new Pair<>("gender", "1"));
        HBaseUtils.putRow(TABLE_NAME, "rowKey3", STUDENT, pairs3);
    }

    public void getRow() {
        Result result = HBaseUtils.getRow(TABLE_NAME, "rowKey1");
        if (result != null) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes(STUDENT), Bytes.toBytes("name"))));
        }

    }

    public void getCell() {
        String cell = HBaseUtils.getCell(TABLE_NAME, "rowKey2", STUDENT, "age");
        System.out.println("cell age :" + cell);

    }

    public void getScanner() {
        ResultScanner scanner = HBaseUtils.getScanner(TABLE_NAME);
        if (scanner != null) {
            scanner.forEach(result -> System.out.println(Bytes.toString(result.getRow()) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(STUDENT), Bytes.toBytes("name")))));
            scanner.close();
        }
    }

    public void getScannerWithFilter() {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes(STUDENT),
                Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("Jack"));
        filterList.addFilter(nameFilter);
        ResultScanner scanner = HBaseUtils.getScanner(TABLE_NAME, filterList);
        if (scanner != null) {
            scanner.forEach(result -> System.out.println(Bytes.toString(result.getRow()) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(STUDENT), Bytes.toBytes("name")))));
            scanner.close();
        }
    }

    public void deleteColumn() {
        boolean b = HBaseUtils.deleteColumn(TABLE_NAME, "rowKey2", STUDENT, "age");
        System.out.println("删除结果: " + b);
    }

    public void deleteRow() {
        boolean b = HBaseUtils.deleteRow(TABLE_NAME, "rowKey2");
        System.out.println("删除结果: " + b);
    }

    public void deleteTable() {
        boolean b = HBaseUtils.deleteTable(TABLE_NAME);
        System.out.println("删除结果: " + b);
    }

    private static void startSsh() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "service ssh start");
            printResult(commandResults);
        }
    }

    private static void startZookeepers() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startZookeeper(i);
        }
    }

    private static void startZookeeper(int serverId) {
        String command = "cd " + ReditHelper.getZookeeperHomeDir() + " && bin/zkServer.sh start";
        logger.info("server" + serverId + " startZookeeper...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void checkZookeepersStatus() throws InterruptedException, RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "cd " + ReditHelper.getZookeeperHomeDir() + " && bin/zkServer.sh status";
            logger.info("server" + i + " checkStatus...");
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, command);
            printResult(commandResults);
            Thread.sleep(1000);
        }
    }


    private static void addZooCfgFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(ZooCfgFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("server")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + ZooCfgFile);
        }
        else {
            BufferedWriter out = new BufferedWriter(new FileWriter(ZooCfgFile, true));
            out.write(ZooCfgConf);
            out.close();
            logger.info("add config to " + ZooCfgFile + ":\n" + ZooCfgConf);
        }
    }

    private static void addHbaseSiteConf() {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(HbaseSiteFile);
            Element properties = doc.getDocumentElement();
            NodeList nodeList = doc.getElementsByTagName("name");
            for(int i = 0; i < nodeList.getLength(); i++){
                Node node = nodeList.item(i);
                String text = node.getTextContent();
                if("hbase.zookeeper.quorum".equals(text)){
                    return;
                }
            }

            FileOutputStream outputStream = new FileOutputStream(HbaseSiteFile);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
            Node property = doc.createElement("property");
            Node name = doc.createElement("name");
            name.setTextContent("hbase.zookeeper.quorum");
            Node value = doc.createElement("value");
            value.setTextContent(HbaseSiteConf);
            property.appendChild(name);
            property.appendChild(value);
            properties.appendChild(property);

            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StreamResult result = new StreamResult(outputStreamWriter);
            DOMSource source = new DOMSource(doc);
            transformer.transform(source, result);
            outputStreamWriter.flush();
            outputStreamWriter.close();

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void addRegionConf() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(RegionFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("10.")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + RegionFile);
        }
        else {
            BufferedWriter out = new BufferedWriter(new FileWriter(RegionFile, true));
            out.write(RegionConf);
            out.close();
            logger.info("add config to " + RegionFile + ":\n" + RegionFile);
        }
    }

    private void checkJps() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            CommandResults commandResults = runner.runtime().runCommandInNode("server" + i, "jps");
            printResult(commandResults);
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
