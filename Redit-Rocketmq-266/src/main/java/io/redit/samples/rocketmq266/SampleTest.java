package io.redit.samples.rocketmq266;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String broker_a = "broker-a.properties";
    private static String broker_a_s = "broker-a-s.properties";
    private static String broker_b = "broker-b.properties";
    private static String broker_b_s = "broker-b-s.properties";
    private static String namesrvAddr = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        namesrvAddr = runner.runtime().ip("server1") + ":9876;" + runner.runtime().ip("server2") + ":9876";
        addRocketPropFile();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException, MQClientException {
        logger.info("wait for Rocketmq ...");
        startServers();
        Thread.sleep(10000);
        startBroker(1, "a");
        Thread.sleep(10000);
        startBroker(2, "a-s");
        Thread.sleep(10000);
        startBroker(2, "b");
        Thread.sleep(10000);
        startBroker(1, "b-s");
        Thread.sleep(10000);
        checkJps();
        startConsumer();
        logger.info("completed !!!");
    }


    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && bin/mqnamesrv > ./logs/mqnamesrv.log 2>&1";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startBroker(int serverId, String broker_id) {
        String command = "cd " + ReditHelper.getRocketmqHomeDir() + " && bin/mqbroker -c ./conf/2m-2s-async/broker-" + broker_id + ".properties > ./logs/broker-" + broker_id + ".log 2>&1";
        logger.info("server" + serverId + " startBroker, broker_id: " + broker_id);
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startConsumer() throws MQClientException {
        logger.info("startConsumer !!!");
        //消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
        //消费者从NameServer拿到topic的queue所在的Broker地址，多个用分号隔开
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeThreadMax(10);
        //设置Consumer第一次启动是从队列头部开始消费
        //如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //subscribe订阅的第一个参数就是topic,第二个参数为生产者发送时候的tags，*代表匹配所有消息，
        //想要接收具体消息时用||隔开，如"TagA||TagB||TagD"
        consumer.subscribe("test_topic", "*");
        //Consumer可以用两种模式启动，广播（Broadcast）和集群（Cluster），广播模式下，一条消息会发送给所有Consumer，
        //集群模式下消息只会发送给一个Consumer
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //批量消费,每次拉取10条
        consumer.setConsumeMessageBatchMaxSize(10);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            //msgs是一个List，一般是Consumer先启动，所有每次都是一条数据
            //如果Producer先启动Consumer端后启动，会积压数据，此时setConsumeMessageBatchMaxSize会生效,
            StringBuilder sb = new StringBuilder();
            sb.append("Listen Message msgs条数：" + msgs.size());
            MessageExt messageExt = msgs.get(0);
            //消息重发了三次
            if (messageExt.getReconsumeTimes() == 3) {
                //todo 持久化消息记录表
                //重试了三次不再重试了，直接签收掉
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }

            for (MessageExt msg : msgs) {
                try {
                    String topic = msg.getTopic();
                    String messageBody = new String(msg.getBody(), "utf-8");
                    String tags = msg.getTags();
                    //todo 业务逻辑处理
                    sb.append(", topic:" + topic + ",tags:" + tags + ",msg:" + messageBody);
                } catch (Exception e) {
                    e.printStackTrace();
                    // 重新消费
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            System.out.println(sb.toString());
            //签收，这句话告诉broker消费成功，可以更新offset了，也就是发送ack。
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }

    private static void addRocketPropFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/" + broker_a));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("namesrvAddr=")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config !!!");
        }
        else {
            BufferedWriter out1 = new BufferedWriter(new FileWriter("conf/" + broker_a, true));
            BufferedWriter out2 = new BufferedWriter(new FileWriter("conf/" + broker_a_s, true));
            BufferedWriter out3 = new BufferedWriter(new FileWriter("conf/" + broker_b, true));
            BufferedWriter out4 = new BufferedWriter(new FileWriter("conf/" + broker_b_s, true));
            out1.write("namesrvAddr=" + namesrvAddr);
            out2.write("namesrvAddr=" + namesrvAddr);
            out3.write("namesrvAddr=" + namesrvAddr);
            out4.write("namesrvAddr=" + namesrvAddr);
            out1.close();
            out2.close();
            out3.close();
            out4.close();
            logger.info("add config !!!" );
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
