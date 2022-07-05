package io.redit.samples.benchmark.hbase;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
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
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for zookeeper...");
        startZookeepers();
        Thread.sleep(5000);
        checkZookeepersStatus();

        logger.info("wait for hbase...");
        startSsh();
        Thread.sleep(2000);
        startHbases();
        Thread.sleep(10000);
        checkJps();

        logger.info("wait for createTable and insertTable...");
        createTable();
        Thread.sleep(5000);
        insertTable();

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

    private static Connection createConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("conf/hbase-site.xml"));
        conf.addResource(new Path("conf/core-site.xml"));
//        conf.set("hbase.zookeeper.quorum", HbaseSiteConf);
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    private static void createNamespace(Connection connection, String tablespace) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(tablespace).build());
        System.out.println("成功创建表空间 " + tablespace);
    }

    private static void create(Connection connection, String tableName, String... columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        if (tableName == null || columnFamilies == null) {
            return;
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < columnFamilies.length; i++) {
            if (columnFamilies[i] == null)
                continue;
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilies[i]);
            columnDescriptor.setMaxVersions(1);
            table.addFamily(columnDescriptor);
        }
        admin.createTable(table);
        System.out.println("成功创建表 " + table + ", column family: " + Arrays.toString(columnFamilies));
    }

    private static void createTable() {
        try (Connection connection = createConnection()) {
            createNamespace(connection,"zmb");
            Thread.sleep(5000);
            create(connection,"zmb:student", "name", "info", "score");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void insert(Connection connection, String tableName, String rowKey, String columnFamily, String column,
                              String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
    }

    public static void scan(Connection connection, String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result tmp;
        System.out.println("Row\t\t\tColumn\tvalue");
        while ((tmp = scanner.next()) != null) {
            List<Cell> cells = tmp.listCells();
            for (Cell cell : cells) {
                String rk = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(rk + "\t\tcolumn:" + cf + ":" + column + ",value=" + value);
            }
        }
    }

    private static void insertTable() {
        try (Connection connection = createConnection()) {
            insert(connection, "zmb:student", "row1", "name", "", "Tom");
            insert(connection, "zmb:student", "row1", "info", "student_id", "20210000000001");
            insert(connection, "zmb:student", "row1", "info", "class", "1");
            insert(connection, "zmb:student", "row1", "score", "understanding", "75");
            insert(connection, "zmb:student", "row1", "score", "programming", "82");

            insert(connection, "zmb:student", "row2", "name", "", "Jerry");
            insert(connection, "zmb:student", "row2", "info", "student_id", "20210000000002");
            insert(connection, "zmb:student", "row2", "info", "class", "1");
            insert(connection, "zmb:student", "row2", "score", "understanding", "85");
            insert(connection, "zmb:student", "row2", "score", "programming", "67");


            insert(connection, "zmb:student", "row3", "name", "", "Jack");
            insert(connection, "zmb:student", "row3", "info", "student_id", "20210000000003");
            insert(connection, "zmb:student", "row3", "info", "class", "2");
            insert(connection, "zmb:student", "row3", "score", "understanding", "80");
            insert(connection, "zmb:student", "row3", "score", "programming", "80");

            Thread.sleep(5000);
            scan(connection,"zmb:student");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
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
