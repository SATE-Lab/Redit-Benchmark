package io.redit.samples.cassandra13464;

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

    @BeforeClass
    public static void before() throws RuntimeEngineException, IOException {
        runner = ReditRunner.run(ReditHelper.getDeployment());
        ReditHelper.startNodesInOrder(runner);
        makeDirs();
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException {
        logger.info("wait for Cassandra ...");
        startServer(1);
        Thread.sleep(10000);
        startServer(2);
        Thread.sleep(30000);
        checkNetStatus(2);
        checkStatus(1);

        Thread.sleep(3000);
        testCreateMaterializedView();
        logger.info("completed !!!");
    }

    private static void testCreateMaterializedView(){
        Cluster cluster = null;
        Session session = null;
        try {
            // 定义一个cluster类
            cluster = Cluster.builder().addContactPoint(runner.runtime().ip("server1")).build();
            // 获取session对象
            session = cluster.connect();
            // 创建键空间
            String createKeySpaceCQL = "CREATE KEYSPACE if not exists test WITH replication={'class':'SimpleStrategy','replication_factor':3}";
            session.execute(createKeySpaceCQL);
            // 创建列族
            String createTableCQL = "CREATE TABLE if not exists test.test(id text PRIMARY KEY , value1 text , value2 text, value3 text)";
            session.execute(createTableCQL);

            // 插入数据
            String insertCQL1 = "INSERT INTO test.test (id, value1 , value2, value3 ) VALUES ('aaa', 'aaa', 'aaa' ,'aaa')";
            String insertCQL2 = "INSERT INTO test.test (id, value1 , value2, value3 ) VALUES ('bbb', 'bbb', 'bbb' ,'bbb')";
            session.execute(insertCQL1);
            session.execute(insertCQL2);

            // 查询数据
            String queryCQL1 = "SELECT token(id),id,value1 FROM test.test";
            ResultSet rs = session.execute(queryCQL1);
            List<Row> dataList = rs.all();
            for (Row row : dataList) {
                System.out.println(row.toString());
            }

            String createMaterializedViewCQL1 = "CREATE MATERIALIZED VIEW test.test_view AS SELECT value1, id FROM test.test WHERE id IS NOT NULL AND value1 IS NOT NULL AND TOKEN(id) > -9223372036854775808 AND TOKEN(id) < -3074457345618258603 PRIMARY KEY(value1, id) WITH CLUSTERING ORDER BY (id ASC)";
            session.execute(createMaterializedViewCQL1);

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

    private static void printResult(CommandResults commandResults){
        logger.info(commandResults.nodeName() + ": " + commandResults.command());
        if (commandResults.stdOut() != null){
            logger.info(commandResults.stdOut());
        }else {
            logger.warn(commandResults.stdErr());
        }
    }
}
