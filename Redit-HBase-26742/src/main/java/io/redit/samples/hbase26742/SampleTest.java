package io.redit.samples.hbase26742;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
import javax.xml.transform.Result;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

import static org.junit.Assert.*;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String HbaseSiteFile = "conf/hbase-site.xml";
    private static String RegionFile = "conf/regionservers";
    private static String ZooCfgConf = "";
    private static String HbaseSiteConf = "";
    private static String RegionConf = "";

    private static Connection connection;
    private static Admin admin = null;
    private static final TableName tableName = TableName.valueOf("t1");
    private static final byte[] FAMILY = Bytes.toBytes("cf");

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
    public static void after() {
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
        Thread.sleep(30000);
        checkJps();
        getConnection();
        Thread.sleep(2000);
        testCheckAndMutateForNull();

        logger.info("completed !!!");
    }

    private static void testCheckAndMutateForNull() throws IOException {
        byte[] qualifier = Bytes.toBytes("Q");
        try (Table table = createTable()) {
            byte[] row1 = Bytes.toBytes("testRow1");
            Put put = new Put(row1);
            put.addColumn(FAMILY, qualifier, Bytes.toBytes("v0"));
            table.put(put);
            assertEquals("v0", Bytes.toString(
                    table.get(new Get(row1).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier)));

            CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row1)
                    .ifMatches(FAMILY, qualifier, CompareOperator.NOT_EQUAL, new byte[]{})
                    .build(new Put(row1).addColumn(FAMILY, qualifier, Bytes.toBytes("v1")));
            table.checkAndMutate(checkAndMutate1);
            assertEquals("v1", Bytes.toString(table.get(new Get(row1).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier)));

            byte[] row2 = Bytes.toBytes("testRow2");
            put = new Put(row2);
            put.addColumn(FAMILY, qualifier, new byte[]{});
            table.put(put);
            assertEquals(0, table.get(new Get(row2).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier).length);

            CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
                    .ifMatches(FAMILY, qualifier, CompareOperator.EQUAL, new byte[]{})
                    .build(new Put(row2).addColumn(FAMILY, qualifier, Bytes.toBytes("v2")));
            table.checkAndMutate(checkAndMutate2);
            assertEquals("v2", Bytes.toString(table.get(new Get(row2).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier)));

            byte[] row3 = Bytes.toBytes("testRow3");
            put = new Put(row3).addColumn(FAMILY, qualifier, Bytes.toBytes("v0"));
            assertNull(table.get(new Get(row3).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier));
            CheckAndMutate checkAndMutate3 = CheckAndMutate.newBuilder(row3)
                    .ifMatches(FAMILY, qualifier, CompareOperator.NOT_EQUAL, new byte[]{})
                    .build(put);
            table.checkAndMutate(checkAndMutate3);
            assertNull(table.get(new Get(row3).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier));

            CheckAndMutate checkAndMutate4 = CheckAndMutate.newBuilder(row3)
                    .ifMatches(FAMILY, qualifier, CompareOperator.EQUAL, new byte[]{})
                    .build(put);
            table.checkAndMutate(checkAndMutate4);
            assertEquals("v0", Bytes.toString(table.get(new Get(row3).addColumn(FAMILY, qualifier)).getValue(FAMILY, qualifier)));
        }
    }

    private static Table createTable() throws IOException {
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
                        .newBuilder(FAMILY).build()).build();
        admin.createTable(tableDescriptor);
        return connection.getTable(tableName);
    }

    private static void getConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", HbaseSiteConf);
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("connection: " + connection);
        System.out.println("admin: " + admin);
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
