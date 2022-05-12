package mapreduce.mr;

import mapreduce.bean.*;
import mapreduce.mrapps.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private Socket socket;
    private ArrayList<KeyValue> intermediate;
    // false on map, true on reduce
    private boolean mapOrReduce;
    // must exit if true
    public boolean allDone;
    public int workerId;

    private static String coordinatorIp;
    private static final String CLUSTER_NAME = "mycluster";
    private static final int NN_HTTP_PORT = 50070;
    private static final int NN_RPC_PORT = 8020;
    private static int numOfNNs = 3;
    private static DistributedFileSystem dfs = null;
    private static String hdfsPath = "hdfs://nn1:8020";
    private static String hdfsOutputFolderPath = hdfsPath + "/output";
    private static String hdfsTempFolderPath = hdfsPath + "/output/temp";

    public Worker(String coordinatorIp) throws IOException {
        logger.info("Worker: making worker");
        this.intermediate = new ArrayList<>();;
        this.mapOrReduce = false;
        this.allDone = false;
        this.workerId = -1;
        this.coordinatorIp = coordinatorIp;
        this.dfs = getDFS();
    }

    /**
     * Worker do map or reduce task.
     */
    public void process() throws Exception {
        if (this.allDone){
            return;
        }
        if (!this.mapOrReduce){
            // process map task
            MapTaskReply reply = this.askMapTask();
            if (reply == null){
                // switch to reduce mode
                this.mapOrReduce = true;
            }else {
                if (reply.getFileId() == -1){
                    // no available tasks for now
                }else {
                    this.executeMap(reply);
                }
            }
        }
        if (this.mapOrReduce){
            // process reduce task
            ReduceTaskReply reply = this.askReduceTask();
            if (reply == null){
                // all done, must exit
                this.allDone = true;
            }else {
                if (reply.getRIndex() == -1){
                    // no available tasks for now
                }else {
                    this.executeReduce(reply);
                }
            }
        }
    }


    /**
     * The worker asks coordinator for a map task.
     * @return a MapTaskReply object.
     */
    private MapTaskReply askMapTask() throws IOException {
        MapTaskArgs args = new MapTaskArgs();
        args.setWorkerId(this.workerId);
        MapTaskReply reply = new MapTaskReply();
        logger.info("Worker: requesting for map task...");
        reply = (MapTaskReply) call("GiveMapTask", args.getParamTypes(), args, reply);
        this.workerId = reply.getWorkerId();

        if (reply.getFileId() == -1){
            // refused to give a task, notify the caller
            if (reply.isAllDone()){
                logger.info("Worker " + this.workerId + " : no more map tasks, switch to reduce mode");
                return null;
            }else {
                return reply;
            }
        }
        logger.info("Worker " + this.workerId + " : get map task on file " + reply.getFileId());
        return reply;
    }

    /**
     * Execute map task and write into temp folder.
     * @param reply
     * @throws IOException
     */
    private void executeMap(MapTaskReply reply) throws Exception {
        ArrayList<KeyValue> intermediate = makeIntermediateFromFile(reply.getFileStatus());
        logger.info("Worker " + this.workerId + " : writing map results to file");
        writeToFiles(reply.getFileId(), reply.getNReduce(), intermediate);
        this.joinMapTask(reply.getFileId());
    }

    /**
     * Make intermediate arrayList from target file.
     * @param fileStatus
     * @return ArrayList<KeyValue>
     * @throws IOException
     */
    private ArrayList<KeyValue> makeIntermediateFromFile(FileStatus fileStatus) throws IOException {
        FSDataInputStream in = dfs.open(fileStatus.getPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        StringBuffer content = new StringBuffer();
        while((line = br.readLine()) != null){
            content.append(line);
        }
        intermediate = WordCount.map(content);
        Collections.sort(intermediate);
        return intermediate;
    }

    /**
     * Divide the results of the map into reduce and write them into 'nReduce' intermediate mapreduce.files.
     * @param fileId
     * @param nReduce
     * @param intermediate
     * @throws Exception
     */
    private void writeToFiles(int fileId, int nReduce, ArrayList<KeyValue> intermediate) throws Exception {
        ArrayList<ArrayList<KeyValue>> keyValues = new ArrayList<>(nReduce);
        for (int i = 0; i < nReduce; i++){
            keyValues.add(i, new ArrayList<>(0));
        }
        for (KeyValue kv : intermediate){
            int index = keyReduceIndex(kv.getKey(), nReduce);
            keyValues.get(index).add(kv);
        }
        for (int i = 0; i < nReduce; i++){
            Path tempPath = getTempPath(fileId, i);
            FSDataOutputStream out = dfs.create(tempPath);
            for (KeyValue kv : keyValues.get(i)){
                out.write((kv.getKey() + " " + kv.getValue() + "\r\n").getBytes("UTF-8"));
            }
            out.close();
            logger.info("Worker " + this.workerId + " : write map result to file " + tempPath.getName());
        }
    }

    /**
     * Choose the reduce task number for each KeyValue emitted by Map.
     * @param key
     * @param nReduce
     * @return
     */
    private int keyReduceIndex(String key, int nReduce) {
        return Math.abs(key.hashCode() % nReduce);
    }

    /**
     * The worker asks coordinator for joining map task.
     * @param fileId
     */
    private void joinMapTask(int fileId) throws IOException {
        MapTaskJoinArgs args = new MapTaskJoinArgs();
        args.setFileId(fileId);
        args.setWorkerId(this.workerId);
        MapTaskJoinReply reply = new MapTaskJoinReply();
        logger.info("Worker " + this.workerId + " : join map task...");
        reply = (MapTaskJoinReply) call("JoinMapTask", args.getParamTypes(), args, reply);

        if (reply.isAccept()){
            logger.info("Worker " + this.workerId + " : map task accepted");
        }else {
            logger.info("Worker " + this.workerId + " : map task not accepted");
        }
    }

    /**
     * The worker asks coordinator for a reduce task.
     * @return
     */
    private ReduceTaskReply askReduceTask() throws IOException {
        ReduceTaskArgs args = new ReduceTaskArgs();
        args.setWorkerId(this.workerId);
        ReduceTaskReply reply = new ReduceTaskReply();
        logger.info("Worker " + this.workerId + " : requesting for reduce task...");
        reply = (ReduceTaskReply) call("GiveReduceTask", args.getParamTypes(), args, reply);

        if (reply.getRIndex() == -1){
            // refused to give a task, notify the caller
            if (reply.isAllDone()){
                logger.info("Worker " + this.workerId + " : no more reduce tasks, try to terminate worker");
                return null;
            }else {
                logger.info("Worker " + this.workerId + " : there is no task available for now. there will be more just a moment...");
                return reply;
            }
        }
        logger.info("Worker " + this.workerId + " : get " + reply.getRIndex() + "th reduce task");
        return reply;
    }

    /**
     * Execute reduce task and write results into output file.
     * @param reply
     * @throws IOException
     */
    private void executeReduce(ReduceTaskReply reply) throws Exception {
        Path outputPath = getOutputPath(reply.getRIndex());
        ArrayList<KeyValue> intermediate = new ArrayList<>(0);
        for (int i = 0; i < reply.getFileCount(); i++){
            logger.info("Worker " + this.workerId + " : add reduce intermediates on file" + i);
            intermediate.addAll(readIntermediates(i, reply.getRIndex()));
        }
        logger.info("Worker " + this.workerId + " : total intermediate count " + intermediate.size());
        reduceKVSlice(intermediate, outputPath);
        this.joinReduceTask(reply.getRIndex());
    }

    /**
     * Read a reduce slice from all file.
     * @param fileId
     * @param reduceId
     * @return a reduce slice in a file.
     * @throws Exception
     */
    private ArrayList<KeyValue> readIntermediates(int fileId, int reduceId) throws Exception {
        ArrayList<KeyValue> keyValues = new ArrayList<>(0);
        Path readPath = getTempPath(fileId, reduceId);
        FSDataInputStream in = dfs.open(readPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        while((line = br.readLine()) != null){
            String[] strings = line.split(" ");
            if (strings.length == 2){
                keyValues.add(new KeyValue(strings[0], strings[1]));
            }
        }
        br.close();
        in.close();
        return keyValues;
    }

    /**
     * Sort intermediate list and write to output file.
     * @param intermediate
     * @param outputPath
     * @throws IOException
     */
    private void reduceKVSlice(ArrayList<KeyValue> intermediate, Path outputPath) throws IOException {
        Collections.sort(intermediate);
        FSDataOutputStream out = dfs.create(outputPath);
        int i = 0, j;
        while (i < intermediate.size()){
            j = i + 1;
            while (j < intermediate.size() && intermediate.get(j).getKey().equals(intermediate.get(i).getKey())){
                j++;
            }
            ArrayList<String> values = new ArrayList<>();
            for (int k = i; k < j; k++){
                values.add(intermediate.get(k).getValue());
            }
            int count = WordCount.reduce(values);
            String data = intermediate.get(i).getKey() + " " + count + "\r\n";
            out.write(data.getBytes("UTF-8"));
            i = j;
        }
        out.close();
    }

    /**
     * The worker asks coordinator for joining reduce task.
     * @param rIndex
     */
    private void joinReduceTask(int rIndex) throws IOException {
        ReduceTaskJoinArgs args = new ReduceTaskJoinArgs();
        args.setRIndex(rIndex);
        args.setWorkerId(this.workerId);
        ReduceTaskJoinReply reply = new ReduceTaskJoinReply();
        reply = (ReduceTaskJoinReply) call("JoinReduceTask", args.getParamTypes(),args, reply);

        if (reply.isAccept()){
            logger.info("Worker " + this.workerId + " : reduce task accepted");
        }else {
            logger.info("Worker " + this.workerId + " : reduce task not accepted");
        }
    }

    /**
     * Get temp files paths.
     * @param fileId
     * @param reduceId
     * @return Path object.
     * @throws IOException
     */
    private Path getTempPath(int fileId, int reduceId) {
        Path writePath = new Path(hdfsTempFolderPath + File.separator + "mapreduce.mr-temp-" + fileId + "-" + reduceId);
        return writePath;
    }

    /**
     * Get output file paths.
     * @param RIndex
     * @return Path object.
     * @throws IOException
     */
    private Path getOutputPath(int RIndex) {
        Path outputPath = new Path(hdfsOutputFolderPath + File.separator + "mapreduce.mr-out-" + RIndex);
        return outputPath;
    }

    /**
     * Send an RPC request to the coordinator, wait for the response.
     * Usually returns true, returns false if something goes wrong.
     * @param rpcName
     * @param args
     * @param reply
     * @return a TaskReply object.
     * @throws IOException
     */
    private TaskReply call(String rpcName, Class<?>[] paramTypes, TaskArgs args, TaskReply reply) throws IOException {
        socket = new Socket(coordinatorIp,12000);
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            List<Object> list = new ArrayList<>();
            list.add(rpcName);
            list.add(paramTypes);
            list.add(args);
            list.add(reply);
            outputStream.writeObject(list);
            outputStream.flush();
            ObjectInputStream inputStream = new ObjectInputStream(in);
            Object res = inputStream.readObject();
            if (res instanceof TaskReply) {
                reply = (TaskReply) res;
            } else {
                throw new RuntimeException("incorrect return object !!!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            out.close();
            in.close();
            return reply;
        }
    }


    /**
     * Example function to show how to make an RPC call to the coordinator.
     * The RPC argument and reply types are defined in rpc.go.
     */
    public void CallExample() throws IOException {
        ExampleArgs args = new ExampleArgs();
        args.setX(99);
        ExampleReply reply = new ExampleReply();
        reply = (ExampleReply) call("CallExample", args.getParamTypes(), args, reply);
        logger.info("Worker " + this.workerId + " : reply.getY() = " + reply.getY());
    }


    /**
     * Get dfs object to use hdfs.
     * @return
     * @throws IOException
     */
    public static DistributedFileSystem getDFS() throws IOException {
        FileSystem fs = FileSystem.get(getConfiguration());
        if (!(fs instanceof DistributedFileSystem)) {
            throw new IllegalArgumentException("FileSystem " + fs.getUri() + " is not an HDFS file system");
        } else {
            return (DistributedFileSystem)fs;
        }
    }

    public static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);
        conf.set("dfs.client.failover.proxy.provider."+ CLUSTER_NAME,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.nameservices", CLUSTER_NAME);
        conf.set("dfs.ha.namenodes."+ CLUSTER_NAME, getNNString());

        conf.set("dfs.namenode.rpc-address."+ CLUSTER_NAME +".nn1", coordinatorIp + ":" + NN_RPC_PORT);
        conf.set("dfs.namenode.http-address."+ CLUSTER_NAME +".nn1", coordinatorIp + ":" + NN_HTTP_PORT);
        return conf;
    }

    private static String getNNString() {
        StringJoiner stringJoiner = new StringJoiner(",");
        for (int i=1; i<=numOfNNs; i++) {
            stringJoiner.add("nn" + i);
        }
        return stringJoiner.toString();
    }

}
