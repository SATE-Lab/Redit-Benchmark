package io.redit.samples.kafka7192_sequence;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import io.redit.samples.kafka7192.TopicCreator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.omg.PortableServer.THREAD_POLICY_ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String ZooCfgFile = "conf/zoo.cfg";
    private static String KafkaPropFile = "server.properties";
    private static String ZooCfgConf = "";
    private static String KafkaPropConf = "zookeeper.connect=";

    private static String BROKER = null;
    private static final String STATE_STORE_NAME = "some-state-store";
    private static final String SOURCE_TOPIC_NAME = "source-topic";
    private static final String SINK_TOPIC_NAME = "output-sink";

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
        BROKER = "PLAINTEXT://" + runner.runtime().ip("server1") + ":9092";
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
            logger.info("Create topic: " + SOURCE_TOPIC_NAME + ", " + SINK_TOPIC_NAME);
            TopicCreator creator = new TopicCreator(BROKER);
            creator.create(SOURCE_TOPIC_NAME);
            creator.create(SINK_TOPIC_NAME);
        });

        runner.runtime().enforceOrder("t2", () -> {
            logger.info("Make Topology ...");
            makeTopology();
        });

        runner.runtime().enforceOrder("t3", () -> {
            logger.info("Make Client ...");
            makeClient();
        });

        Thread.sleep(10000);

        runner.runtime().enforceOrder("t4", () -> {
            logger.info("Make Topology ...");
            makeTopology();
        });

        Thread.sleep(10000);
        runner.runtime().waitForRunSequenceCompletion(20);
        logger.info("completed !!!");
    }


    private static void makeClient(){
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        String messageId = UUID.randomUUID().toString();
        logger.info("=========== Starting test with message-id: " + messageId + " ===========");
        ProducerRecord<String, String> first = new ProducerRecord<>("source-topic", messageId, messageId);
        producer.send(first);
        producer.flush();
        producer.close();

        logger.info("The streams topology should have crashed by design.");
        logger.info("Specifically, before any record was published to the output topic");
        logger.info("If the rocks-db state store is behaving correctly, the state store should be empty upon restart...");
        logger.info("Restart the topology, and the message is reprocessed (good) but the state-store has persisted...");
    }

    private static void makeTopology() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "statestoresadness");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/statestoresadness/");

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                Serdes.String(),
                Serdes.String());

        Topology topology = new StreamsBuilder()
                .build()
                .addSource("source", SOURCE_TOPIC_NAME)
                .addProcessor("processor", () -> new AbstractProcessor<String, String>() {

                    KeyValueStore<String, String> stateStore;

                    @Override
                    public void process(final String key, final String value) {

                        String storedKey = stateStore.get(key);
                        logger.info("=========== BEGIN ALL STORED VALUES ===========");
                        stateStore.all().forEachRemaining(x -> System.out.println(x.key + " : " + x.value));
                        logger.info("=========== END ALL STORED VALUES ===========");

                        if(storedKey != null) {
                            logger.info("=========== " + key + " : " + value + " was found in the state store!!! ===========");
                            System.exit(0);
                        }

                        logger.info("=========== Message " + key + " was not a duplicate, lets register is in the state store ===========");
                        stateStore.put(key, value);
                        stateStore.flush();
                        logger.info("Now to throw an error before forwarding the message...");

                        if (true) {
                            throw new RuntimeException("Some random error...");
                        }

                        ///This never happens, of course....
                        this.context().forward(key, value);
                    }

                    @Override
                    public void init(ProcessorContext context) {
                        super.init(context);
                        stateStore = (KeyValueStore<String, String>)context().getStateStore(STATE_STORE_NAME);
                    }
                }, "source")

                .addSink("sink", SINK_TOPIC_NAME, "processor")
                .addStateStore(storeBuilder, "processor");

        KafkaStreams streams = new KafkaStreams(topology, config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
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
