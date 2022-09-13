package io.redit.samples.hbase21487;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ConcurrentTableModificationException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
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
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

import static org.junit.Assert.assertTrue;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String HbaseSiteFile = "conf/hbase-site.xml";
    private static String RegionFile = "conf/regionservers";
    private static String HostsFile = "conf/hosts";
    private static String ZooCfgConf = "";
    private static String HbaseSiteConf = "";
    private static String RegionConf = "";
    private static String HostsConf = "";

    private static Connection connection;
    private static Admin admin = null;
    private static final byte[] column_Family1 = Bytes.toBytes("A");
    private static final byte[] column_Family2 = Bytes.toBytes("B");
    private static final byte[] column_Family3 = Bytes.toBytes("C");
    private static final int REGION_COUNT = 5;
    private static final long ROW_COUNT = 20;
    private static final int ROW_LENGTH = 20;

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
            HostsConf += runner.runtime().ip("server" + (i)) + "\tserver" + i;
            if (i < ReditHelper.numOfServers){
                HbaseSiteConf += ",";
                RegionConf += "\n";
                HostsConf += "\n";
            }

        }
        addZooCfgFile();
        addHbaseSiteConf();
        addRegionConf();
        addHostsConf();
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
        Thread.sleep(50000);
        checkJps();
        getConnection();
        Thread.sleep(5000);
        testConcurrentAddColumnFamily();

        logger.info("completed !!!");
    }

    private static void testConcurrentAddColumnFamily() throws IOException, InterruptedException {
        TableName table = TableName.valueOf("t1");
        createTable(table);

        class ConcurrentAddColumnFamily extends Thread {
            TableName tableName;
            HColumnDescriptor hcd;
            boolean exception;

            public ConcurrentAddColumnFamily(TableName tableName, HColumnDescriptor hcd) {
                this.tableName = tableName;
                this.hcd = hcd;
                this.exception = false;
            }

            public void run() {
                try {
                    admin.addColumnFamily(tableName, hcd);
                } catch (Exception e) {
                    if (e.getClass().equals(ConcurrentTableModificationException.class)) {
                        this.exception = true;
                    }
                }
            }
        }
        ConcurrentAddColumnFamily t1 = new ConcurrentAddColumnFamily(table, new HColumnDescriptor(column_Family2));
        ConcurrentAddColumnFamily t2 = new ConcurrentAddColumnFamily(table, new HColumnDescriptor(column_Family3));
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        int lengthOfColumnFamilies = admin.getDescriptor(table).getColumnFamilies().length;
        System.out.println("length Of Column Families: " + lengthOfColumnFamilies);
        ColumnFamilyDescriptor[] columnFamilyDescriptors = admin.getDescriptor(table).getColumnFamilies();
        for (ColumnFamilyDescriptor columnFamilyDescriptor: columnFamilyDescriptors){
            System.out.println("columnFamily: " + columnFamilyDescriptor.getNameAsString());
        }
        assertTrue("Expected ConcurrentTableModificationException.", ((t1.exception || t2.exception) && lengthOfColumnFamilies == 2) || lengthOfColumnFamilies == 3);
    }

    private static Table createTable(TableName tableName) throws IOException {
        TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(column_Family1).setBlocksize(1024 * 4).build()).build();
        byte[][] splits = new byte[REGION_COUNT - 1][];
        for (int i = 1; i < REGION_COUNT; i++) {
            splits[i - 1] = Bytes.toBytes(buildRow((int) (ROW_COUNT / REGION_COUNT * i)));
        }
        admin.createTable(td);
        return connection.getTable(tableName);
    }

    private static String buildRow(int index) {
        String value = Long.toString(index);
        String prefix = "user";
        for (int i = 0; i < ROW_LENGTH - value.length(); i++) {
            prefix += '0';
        }
        return prefix + value;
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

    private static void addHostsConf() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader(HostsFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("10.")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + HostsFile);
        }
        else {
            BufferedWriter out = new BufferedWriter(new FileWriter(HostsFile, true));
            out.write(HostsConf);
            out.close();
            logger.info("add config to " + HostsFile + ":\n" + HostsConf);
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
