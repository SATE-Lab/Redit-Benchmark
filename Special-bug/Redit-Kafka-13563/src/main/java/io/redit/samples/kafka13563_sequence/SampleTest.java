package io.redit.samples.kafka13563_sequence;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String KafkaPropFile = "server.properties";
    private static String ZooCfgConf = "";
    private static String KafkaPropConf = "zookeeper.connect=";
    private static String BOOTSTRAP_SERVERS = "";
    private static final String TEST_TOPIC = "TEST";
    private static final String GROUP_ID = "TestGroup";
    private static int kafkaControllerId = -1;
    private static int firstShutdownServerId = -1;
    private static int secondShutdownServerId = -1;
    private static ArrayList<String> kafkaAliveServerIds = new ArrayList<>(Arrays.asList("1", "2", "3"));

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            ZooCfgConf += "server." + i + "=" + runner.runtime().ip("server" + (i)) + ":2888:3888\n";
            KafkaPropConf += runner.runtime().ip("server" + (i)) + ":2181";
            if (i < ReditHelper.numOfServers){
                KafkaPropConf += ",";
            }
        }
        BOOTSTRAP_SERVERS = runner.runtime().ip("server1") + ":9092," + runner.runtime().ip("server2") + ":9092," + runner.runtime().ip("server3") + ":9092";
        addZooCfgFile();
        addKafkaPropFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException, TimeoutException {
        logger.info("wait for zookeeper...");
        startZookeepers();
        Thread.sleep(10000);
        checkZookeepersStatus();

        logger.info("wait for kafka...");
        startKafkas();
        Thread.sleep(10000);
        checkJps();

        runner.runtime().enforceOrder("t1", () -> {
            createTopic(1);
        });

        runner.runtime().enforceOrder("t2", () -> {
            testConsumerAssign();
        });

        findController();
        firstShutdownServerId = kafkaControllerId + 1;

        runner.runtime().enforceOrder("t3", () -> {
            shutdownBroker(firstShutdownServerId);
        });

        Thread.sleep(10000);
        findController();
        secondShutdownServerId = kafkaControllerId + 1;

        runner.runtime().enforceOrder("t4", () -> {
            startBroker(firstShutdownServerId);
        });

        Thread.sleep(10000);
        findController();

        runner.runtime().enforceOrder("t5", () -> {
            shutdownBroker(secondShutdownServerId);
        });

        Thread.sleep(20000);
        findController();
        runner.runtime().waitForRunSequenceCompletion(20);
        logger.info("completed !!!");
    }

    private void startBroker(int serverId) throws RuntimeEngineException {
        logger.info("============ start kafka service on server " + serverId + " ============");
        kafkaAliveServerIds.add(String.valueOf(serverId));
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-server-start.sh -daemon ./config/server.properties";
        runner.runtime().runCommandInNode("server" + serverId, command);
    }

    private void shutdownBroker(int serverId) throws RuntimeEngineException {
        logger.info("============ shutdown kafka service on server " + serverId + " ============");
        kafkaAliveServerIds.remove(String.valueOf(serverId));
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-server-stop.sh";
        runner.runtime().runCommandInNode("server" + serverId, command);
    }

    private void createTopic(int serverId) throws RuntimeEngineException {
        String dockerName = "server" + serverId;
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-topics.sh --bootstrap-server " + BOOTSTRAP_SERVERS + " --create --replication-factor 3 --partitions 2 --topic " + TEST_TOPIC;
        CommandResults commandResults = runner.runtime().runCommandInNode(dockerName, command);
        printResult(commandResults);
    }

    private void findController(){
        List<String> commands= new ArrayList<>();
        String id = kafkaAliveServerIds.get(0);
        commands.add("kafkacat -b " + runner.runtime().ip("server" + id) + ":9092 -L");
        kafkaControllerId = executeNewFlow(commands);
        logger.info("kafkaControllerId: " + kafkaControllerId);
    }

    private void testConsumerAssign(){
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        Properties properties1 = new Properties();
        properties1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties1.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties1.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        final KafkaConsumer<Void, String> consumer = new KafkaConsumer<>(properties1, new VoidDeserializer(), new StringDeserializer());
        consumer.assign(Arrays.asList(new TopicPartition(TEST_TOPIC, 0)));
        executor.execute(() -> {
            while (true) {
                consumer.poll(Duration.ofSeconds(1)).forEach(record -> logger.info("============" + record.toString() + "============" ));
            }
        });

        Properties properties2 = new Properties();
        properties2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties2.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        final KafkaProducer<Void, String> producer = new KafkaProducer<>(properties2, new VoidSerializer(), new StringSerializer());

        executor.scheduleWithFixedDelay(() -> producer.send(new ProducerRecord<>(TEST_TOPIC, Instant.now().toString())), 1, 1, TimeUnit.SECONDS);
    }

    private void startKafkas() {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startKafka(i);
        }
    }

    private static void startKafka(int serverId) {
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-server-start.sh -daemon ./config/server.properties";
        logger.info("server" + serverId + " startKafka...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
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

    private static void addKafkaPropFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + KafkaPropFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("zookeeper.connect=")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + KafkaPropFile);
        }
        else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++){
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + KafkaPropFile, true));
                String listenerConf = "listeners = PLAINTEXT://" + runner.runtime().ip("server" + i) + ":9092\n";
                out.write(listenerConf);
                out.write(KafkaPropConf);
                out.close();
                logger.info("add config to " + KafkaPropFile + ":\n" + KafkaPropConf);
            }
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

    //服务器执行命令行方法
    public int executeNewFlow(List<String> commands) {
        int brokerId = -1;
        Runtime run = Runtime.getRuntime();
        try {
            Process proc = run.exec("/bin/bash", null, null);
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
            for (String line : commands) {
                out.println(line);
            }
            out.println("exit");// 这个命令必须执行，否则in流不结束。
            String rspLine = "";
            logger.info("============ kafkacat :" + commands + "============");
            while ((rspLine = in.readLine()) != null) {
                logger.info(rspLine);
                if(rspLine.indexOf("controller") != -1){
                    int index = rspLine.indexOf("broker") + 7;
                    brokerId = Integer.parseInt(rspLine.substring(index, index+1));
                }
            }
            proc.waitFor();
            in.close();
            out.close();
            proc.destroy();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return brokerId;
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
