package io.redit.samples.kafka13937;

import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.SaslConfigs;
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

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static AdminClient adminClient = null;

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
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
        logger.info("wait for kafka...");
        formatAllStorage();
        Thread.sleep(5000);
        startKafkas();
        Thread.sleep(10000);
        checkJps();

        getAdminClient(runner.runtime().ip("server1") + ":9092");
        Thread.sleep(5000);
        addACLAuth("test", "writer");
        Thread.sleep(5000);
        deleteACLAuth("test", "writer");
        logger.info("completed !!!");
    }

    public void getAdminClient(String bootstrapServers) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        configs.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"writer\" password=\"writer\";");
        adminClient = AdminClient.create(configs);
    }

    public void addACLAuth(String resourceName, String username) {
        ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
        AccessControlEntry accessControlEntry = new AccessControlEntry("User:" + username, "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(resource, accessControlEntry);

        CreateAclsResult createAclsResult = adminClient.createAcls(Collections.singletonList(aclBinding));
        for (Map.Entry<AclBinding, KafkaFuture<Void>> e : createAclsResult.values().entrySet()) {
            KafkaFuture<Void> future = e.getValue();
            try {
                future.get();
                boolean success = !future.isCompletedExceptionally();
                if (success) {
                    logger.info("创建权限成功");
                }
            } catch (Exception exc) {
                logger.warn("创建权限失败，错误信息：{}", exc.getMessage());
                exc.printStackTrace();
            }
        }
    }

    public void deleteACLAuth(String resourceName, String username) {
        ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
        AccessControlEntry accessControlEntry = new AccessControlEntry("User:" + username, "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(resource, accessControlEntry);
        DeleteAclsResult deleteAclsResult = adminClient.deleteAcls(Collections.singletonList(aclBinding.toFilter()));
        for (Map.Entry<AclBindingFilter, KafkaFuture<DeleteAclsResult.FilterResults>> e : deleteAclsResult.values().entrySet()) {
            KafkaFuture<DeleteAclsResult.FilterResults> future = e.getValue();
            try {
                future.get();
                boolean success = !future.isCompletedExceptionally();
                if (success) {
                    logger.info("删除权限成功");
                }
            } catch (Exception exc) {
                logger.warn("删除权限失败，错误信息：{}", exc.getMessage());
                exc.printStackTrace();
            }
        }
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
