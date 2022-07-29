package io.redit.samples.cassandra14088;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static Cluster cluster = null;
    private static Session session = null;

    @BeforeClass
    public static void before() throws RuntimeEngineException, InterruptedException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        makeDirs();

        logger.info("wait for Cassandra ...");
        startServer(1);
        Thread.sleep(10000);
        startServer(2);
        Thread.sleep(30000);
        checkNetStatus(2);
        checkStatus(1);
        Thread.sleep(3000);
        createTABLE();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
            session.close();
            cluster.close();
        }
    }

    @Test
    public void sampleTest1() {
        try {
            String selectCQL = "SELECT writetime(s) FROM test.test";
            session.execute(selectCQL);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void sampleTest2() {
        try {
            String selectCQL = "SELECT writetime(st) FROM test.test";
            session.execute(selectCQL);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void sampleTest3() {
        try {
            String selectCQL = "SELECT writetime(t) FROM test.test";
            session.execute(selectCQL);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void sampleTest4() {
        try {
            String selectCQL = "SELECT writetime(ft) FROM test.test";
            session.execute(selectCQL);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    private static void createTABLE(){
        // 定义一个cluster类
        cluster = Cluster.builder().addContactPoint(runner.runtime().ip("server1")).build();
        // 获取session对象
        session = cluster.connect();
        // 创建键空间
        String createKeySpaceCQL = "CREATE KEYSPACE if not exists test WITH replication={'class':'SimpleStrategy','replication_factor':3}";
        session.execute(createKeySpaceCQL);
        // 创建TYPE
        String createType = "CREATE TYPE test.udt (a int, b int)";
        session.execute(createType);
        // 创建列族
        String createTableCQL = "CREATE TABLE if not exists test.test (k int PRIMARY KEY, s set<int>, fs frozen<set<int>>, t udt, ft frozen<udt>)";
        session.execute(createTableCQL);
    }

    private static void makeDirs() throws RuntimeEngineException {
        for(int i = 1; i <= ReditHelper.numOfServers; i++){
            String command = "mkdir /opt/cassandra && cd /opt/cassandra && mkdir data_file_directories commitlog_directory saved_caches_directory hints_directory";
            runner.runtime().runCommandInNode("server" + i, command);
        }
    }

    private static void startServer(int serverId) {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/cassandra -R ";
        logger.info("server" + serverId + " startServer...");
        new Thread(() -> {
            try {
                runner.runtime().runCommandInNode("server" + serverId, command);
            } catch (RuntimeEngineException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void checkNetStatus(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool netstats ";
        CommandResults commandResult = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResult);
    }

    private static void checkStatus(int serverId) throws RuntimeEngineException {
        String command = "cd " + ReditHelper.getCassandraHomeDir() + " && bin/nodetool status ";
        CommandResults commandResult = runner.runtime().runCommandInNode("server" + serverId, command);
        printResult(commandResult);
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
