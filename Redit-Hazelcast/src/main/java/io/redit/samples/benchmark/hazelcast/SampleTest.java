package io.redit.samples.benchmark.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws InterruptedException, RuntimeEngineException {
        logger.info("wait for hazelcast...");
        startServers();
        Thread.sleep(20000);
        checkServers();
        Thread.sleep(5000);
        getClient();
        createMap();
        Thread.sleep(5000);
        selectMap();
        logger.info("completed !!!");
    }

    private void checkServers() throws InterruptedException, RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            checkServer(i);
            Thread.sleep(1000);
        }
    }

    private void checkServer(int serverId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && bin/hz-cli --config config/hazelcast-client.xml cluster ";
        logger.info("server" + serverId + " checkServer...");
        logger.info(command);
        new Thread(() -> {
            try {
                CommandResults commandResults = runner.runtime().runCommandInNode("server" + serverId, command);
                printResult(commandResults);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void startServers() throws InterruptedException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            startServer(i);
            Thread.sleep(1000);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getRaftHomeDir() + " && bin/hz-start ";
        logger.info("server" + serverId + " startServer...");
        logger.info(command);
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static HazelcastInstance getClient(){
        System.out.println("test getClient ...");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("hello-world");
        clientConfig.getNetworkConfig().addAddress("10.3.0.4:5701", "10.3.0.3:5701", "10.3.0.2:5701");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        return client;
    }

    private static void createMap(){
        System.out.println("test createMap ...");
        HazelcastInstance client = getClient();
        IMap<Integer, Employee> employeeMap  = client.getMap("employeeMap");
        employeeMap .put(1, new Employee("aa", 18, true));
        employeeMap .put(2, new Employee("bb", 19, false));
        employeeMap .put(3, new Employee("cc", 31, true));
        client.shutdown();
    }

    private static void selectMap(){
        System.out.println("test select Map ...");
        HazelcastInstance client = getClient();
        IMap<Integer, Employee> myMap = client.getMap("employeeMap");
        for (Map.Entry<Integer, Employee> entry : myMap.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue().toString());
        };
        client.shutdown();
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
