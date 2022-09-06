package io.redit.samples.cassandra13669_sequence;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.redit.ReditRunner;
import io.redit.exceptions.RuntimeEngineException;
import io.redit.execution.CommandResults;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeoutException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);
    protected static ReditRunner runner;
    private static String CassandraYamlFile = "cassandra.yaml";
    private static String seedsIp = "";

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        seedsIp = runner.runtime().ip("server1");
        addCassandraYamlFile();
        makeDirs();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException, TimeoutException {
        logger.info("wait for Cassandra ...");
        startServer(1);
        Thread.sleep(10000);
        startServer(2);
        Thread.sleep(30000);
        checkNetStatus(2);
        checkStatus(1);

        runner.runtime().enforceOrder("t1", () -> {
            makeUUID();
        });

        runner.runtime().enforceOrder("t2", () -> {
            runner.runtime().restartNode("server1", 30);
            checkStatus(1);
            startServer(1);
        });

        Thread.sleep(30000);

        runner.runtime().enforceOrder("t3", () -> {
            checkStatus(1);
        });

        runner.runtime().waitForRunSequenceCompletion(20);
        logger.info("completed !!!");
    }

    private static void makeUUID(){
        Cluster cluster = null;
        Session session = null;
        try {
            // 定义一个cluster类
            cluster = Cluster.builder().addContactPoint(runner.runtime().ip("server1")).build();
            // 获取session对象
            session = cluster.connect();
            // 创建键空间
            String createKeySpaceCQL = "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true";
            session.execute(createKeySpaceCQL);
            session.execute("use test");
            // 创建列族
            String createTableCQL = "CREATE TABLE test_table(col1 uuid, col2 uuid, ts timeuuid, col3 uuid, PRIMARY KEY((col1, col2), ts)) with clustering order by (ts desc)";
            session.execute(createTableCQL);

            String createCustomIndexCQL = "CREATE CUSTOM INDEX col3_test_table_idx ON test_table(col3) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 'mode': 'PREFIX'}";
            session.execute(createCustomIndexCQL);

            String insertCQL = "INSERT INTO test_table(col1, col2, ts, col3) VALUES(898e0014-6161-11e7-b9b7-238ea83bd70b, 898e0014-6161-11e7-b9b7-238ea83bd70b, now(), 898e0014-6161-11e7-b9b7-238ea83bd70b)";
            session.execute(insertCQL);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            session.close();
            cluster.close();
        }
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

    private static void addCassandraYamlFile() throws IOException {
        boolean addConf = false;
        BufferedReader in = new BufferedReader(new FileReader("conf/server1/" + CassandraYamlFile));
        String str;
        while ((str = in.readLine()) != null) {
            if (str.startsWith("listen_address")){
                addConf = true;
            }
        }
        in.close();
        if (addConf){
            logger.info("already add config in " + CassandraYamlFile);
        }
        else {
            for (int i = 1; i <= ReditHelper.numOfServers; i++){
                BufferedWriter out = new BufferedWriter(new FileWriter("conf/server" + i + "/" + CassandraYamlFile, true));
                String seeds = "      - seeds: \"" + seedsIp + "\"\n";
                String listen_address = "listen_address: " + runner.runtime().ip("server" + i) + "\n";
                String rpc_address = "rpc_address: " + runner.runtime().ip("server" + i) + "\n";
                out.write(seeds);
                out.write(listen_address);
                out.write(rpc_address);
                out.close();
                logger.info("add config to " + CassandraYamlFile);
            }
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
