package io.redit.samples.kafka13909;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


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
        logger.info("wait for kafka with kraft...");
        formatAllStorage();
        Thread.sleep(5000);
        startKafkas();
        Thread.sleep(10000);
        checkJps();

        testACL();
        logger.info("completed !!!");
    }

    private void testACL(){
        KafkaPrincipal principal = new KafkaPrincipal("User", "my-user");
        AdminClient adminClient = getAdminClient();

        // Create ACL
        AclBinding acl = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
                new AccessControlEntry(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );
        adminClient.createAcls(Collections.singletonList(acl));

        // Delete ACL
        AclBindingFilter acl2 = new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL),
                new AccessControlEntryFilter(principal.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );
        adminClient.deleteAcls(Collections.singletonList(acl2));
        adminClient.close();
    }

    public static AdminClient getAdminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"10.5.0.3:9092");
        properties.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
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
