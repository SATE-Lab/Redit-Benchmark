package io.redit.samples.kafka13996;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static final String topicName = "my-topic";
    private static String bootstrapServer = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        bootstrapServer = runner.runtime().ip("server1") + ":9092";
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for kafka with kraft...");
        formatAllStorage();
        Thread.sleep(5000);
        startKafkas();
        Thread.sleep(20000);
        checkJps();

        createTopic(1);
        changeLogCleaner(1);
        Thread.sleep(5000);
        createConsumer();
        Thread.sleep(5000);
        createProducer();
        Thread.sleep(100000);
        logger.info("completed !!!");
    }

    private void createTopic(int serverId) throws RuntimeEngineException {
        String dockerName = "server" + serverId;
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-topics.sh --bootstrap-server " + bootstrapServer + " --create --replication-factor 1 --partitions 1 --topic " + topicName + " --config cleanup.policy=compact --config cleanup.policy=compact --config segment.bytes=104857600 --config compression.type=producer";
        CommandResults commandResults = runner.runtime().runCommandInNode(dockerName, command);
        printResult(commandResults);
    }

    private void changeLogCleaner(int serverId) throws RuntimeEngineException {
        String dockerName = "server" + serverId;
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-configs.sh --bootstrap-server " + bootstrapServer + " --entity-type brokers --entity-default --alter --add-config log.cleaner.io.max.bytes.per.second=100000";
        CommandResults commandResults = runner.runtime().runCommandInNode(dockerName, command);
        printResult(commandResults);
    }

    public static void createProducer() throws InterruptedException {
        //0 设置配置
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //选择序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        //1 ：建立连接
        org.apache.kafka.clients.producer.KafkaProducer kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(properties);
        //2：发送消息
        for (int i = 0; i < 1000; i++) {
            kafkaProducer.send(new ProducerRecord(topicName, "helloworld " + i));
            Thread.sleep(200);
            System.out.println("Producer put message:  helloworld " + i);
        }
        //3：关闭连接
        kafkaProducer.close();
    }

    public static void createConsumer() {
        new Thread(() -> {
            //0 设置配置
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            //选择反序列化方式
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
            //设置消费者组
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
            //1 ：建立连接
            KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(properties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //2：接收消息
            while (true){
                ConsumerRecords<String,String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
                poll.forEach(dto-> System.out.println("Consumer get message: " + dto));
            }
        }).start();
    }

    private void formatAllStorage() throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-storage.sh random-uuid";
        logger.info("wait for format Storage...");
        CommandResults commandResults = runner.runtime().runCommandInNode("server1", command);
        String uuid = commandResults.stdOut().replaceAll("\n","");
        logger.info("get uuid: " + uuid);
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            formatStorageLog(i, uuid);
        }
    }

    private void formatStorageLog(int serverId, String uuid) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getKafkaHomeDir() + " && bin/kafka-storage.sh format -t " + uuid + " -c  ./config/server.properties";
        CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResults);
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
