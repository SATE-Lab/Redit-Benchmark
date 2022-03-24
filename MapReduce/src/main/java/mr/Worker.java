package mr;

import bean.*;
import mrapps.WordCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private Socket socket;
    private ArrayList<KeyValue> intermediate;
    // false on map, true on reduce
    private boolean mapOrReduce;
    // must exit if true
    public boolean allDone;
    private int workerId;

    public Worker() throws IOException {
        logger.info("making worker");
        this.socket = new Socket("127.0.0.1",12000);
        this.intermediate = new ArrayList<KeyValue>();;
        this.mapOrReduce = false;
        this.allDone = false;
        this.workerId = -1;
    }

    /**
     * Worker do map or reduce task.
     */
    public void process() throws Exception {
        if (this.allDone){
            logger.info("tasks all done");
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
        logger.info("requesting for map task...");
        reply = (MapTaskReply) call("GiveMapTask", args, reply);
        this.workerId = reply.getWorkId();

        if (reply.getFileId() == -1){
            // refused to give a task, notify the caller
            if (reply.isAllDone()){
                logger.info("no more map tasks, switch to reduce mode");
                return null;
            }else {
                return reply;
            }
        }
        logger.info("get map task on file " + reply.getFileId() + " : " + reply.getFile().getName());
        return reply;
    }

    /**
     * Execute map task and write into temp folder.
     * @param reply
     * @throws IOException
     */
    private void executeMap(MapTaskReply reply) throws Exception {
        ArrayList<KeyValue> intermediate = makeIntermediateFromFile(reply.getFile());
        logger.info("writing map results to file");
        writeToFiles(reply.getFileId(), reply.getNReduce(), intermediate);
        this.joinMapTask(reply.getFileId());
    }

    /**
     * Make intermediate arrayList from target file.
     * @param file
     * @return ArrayList<KeyValue>
     * @throws IOException
     */
    private ArrayList<KeyValue> makeIntermediateFromFile(File file) throws IOException {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
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
     * Divide the results of the map into reduce and write them into 'nReduce' intermediate files.
     * @param fileId
     * @param nReduce
     * @param intermediate
     * @throws Exception
     */
    private void writeToFiles(int fileId, int nReduce, ArrayList<KeyValue> intermediate) throws Exception {
        ArrayList<ArrayList<KeyValue>> keyValues = new ArrayList<>(nReduce);
        for (int i = 0; i < nReduce; i++){
            keyValues.add(i, new ArrayList<KeyValue>(0));
        }
        for (KeyValue kv : intermediate){
            int index = keyReduceIndex(kv.getKey(), nReduce);
            keyValues.get(index).add(kv);
        }
        for (int i = 0; i < nReduce; i++){
            File tempFile = createWriteFile(fileId, i);
            FileWriter fileWriter = new FileWriter(tempFile, true);
            for (KeyValue kv : keyValues.get(i)){
                fileWriter.write(kv.getKey() + " " + kv.getValue());
            }
            fileWriter.close();
            logger.info("write map result to file " + tempFile.getName());
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
        MapTaskJoinReply reply = new MapTaskJoinReply();
        logger.info("join map task...");
        reply = (MapTaskJoinReply) call("JoinMapTask", args, reply);

        if (reply.isAccept()){
            logger.info("map task accepted");
        }else {
            logger.info("map task not accepted");
        }
    }

    /**
     * The worker asks coordinator for a reduce task.
     * @return
     */
    private ReduceTaskReply askReduceTask() throws IOException {
        ReduceTaskArgs args = new ReduceTaskArgs();
        args.setWorkId(this.workerId);
        ReduceTaskReply reply = new ReduceTaskReply();
        logger.info("requesting for reduce task...");
        reply = (ReduceTaskReply) call("GiveReduceTask", args, reply);

        if (reply.getRIndex() == -1){
            // refused to give a task, notify the caller
            if (reply.isAllDone()){
                logger.info("no more reduce tasks, try to terminate worker");
                return null;
            }else {
                logger.info("there is no task available for now. there will be more just a moment...");
                return reply;
            }
        }
        logger.info("got reduce task on " + reply.getRIndex() + "th cluster");
        return reply;
    }

    /**
     * Execute reduce task and write results into output file.
     * @param reply
     * @throws IOException
     */
    private void executeReduce(ReduceTaskReply reply) throws Exception {
        File outputFile = getOutputFile(reply.getRIndex());
        ArrayList<KeyValue> intermediate = new ArrayList<KeyValue>(0);
        for (int i = 0; i < reply.getFileCount(); i++){
            logger.info("generating intermediates on cluster " + i);
            intermediate.addAll(readIntermediates(i, reply.getRIndex()));
        }
        logger.info("total intermediate count " + intermediate.size());
        reduceKVSlice(intermediate, outputFile);
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
        ArrayList<KeyValue> keyValues = new ArrayList<KeyValue>(0);
        File readFile = getReadFile(fileId, reduceId);
        FileReader fr = new FileReader(readFile);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while((line = br.readLine()) != null){
            String[] strings = line.split(" ");
            if (strings.length == 2){
                keyValues.add(new KeyValue(strings[0], strings[1]));
            }
        }
        br.close();
        fr.close();
        return keyValues;
    }

    /**
     * Sort intermediate list and write to output file.
     * @param intermediate
     * @param outputFile
     * @throws IOException
     */
    private void reduceKVSlice(ArrayList<KeyValue> intermediate, File outputFile) throws IOException {
        Collections.sort(intermediate);
        FileWriter fileWriter = new FileWriter(outputFile, true);
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
            fileWriter.write(data);
            i = j;
        }
        fileWriter.close();
    }

    /**
     * The worker asks coordinator for joining reduce task.
     * @param rIndex
     */
    private void joinReduceTask(int rIndex) throws IOException {
        ReduceTaskJoinArgs args = new ReduceTaskJoinArgs();
        args.setRIndex(rIndex);
        args.setWorkId(this.workerId);
        ReduceTaskJoinReply reply = new ReduceTaskJoinReply();
        reply = (ReduceTaskJoinReply) call("JoinReduceTask", args, reply);

        if (reply.isAccept()){
            logger.info("reduce task accepted");
        }else {
            logger.info("reduce task not accepted");
        }
    }

    /**
     * Create temp write files.
     * @param fileId
     * @param reduceId
     * @return File object.
     * @throws IOException
     */
    private File createWriteFile(int fileId, int reduceId) throws Exception {
        String userDir = System.getProperty("user.dir");
        File tempDirFile = new File(userDir + File.separator + "output" + File.separator + "temp");
        if (tempDirFile.isDirectory()){
            File writeFile = new File(tempDirFile.getAbsoluteFile() + File.separator + "mr-temp-" + fileId + "-" + reduceId);
            if(!writeFile.exists()){
                writeFile.createNewFile();
            }
            return writeFile;
        }else {
            throw new Exception(tempDirFile + " is not exist, can not write.");
        }
    }

    /**
     * Get reduce input files.
     * @param fileId
     * @param reduceId
     * @return File object.
     * @throws Exception
     */
    private File getReadFile(int fileId, int reduceId) throws Exception {
        String userDir = System.getProperty("user.dir");
        File readFile = new File(userDir + File.separator + "output" + File.separator + "temp" + File.separator + "mr-temp-" + fileId + "-" + reduceId);
        if (readFile.exists()){
            return readFile;
        }else {
            throw new Exception(readFile.getName() + " is not existed, can not open file.");
        }
    }

    /**
     * Get output file object.
     * @param RIndex
     * @return
     * @throws IOException
     */
    private File getOutputFile(int RIndex) throws IOException {
        String userDir = System.getProperty("user.dir");
        File outputFile = new File(userDir + File.separator + "output" + File.separator + "mr-out-" + RIndex);
        if(!outputFile.exists()){
            outputFile.createNewFile();
        }
        return outputFile;
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
    private TaskReply call(String rpcName, TaskArgs args, TaskReply reply) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            List<Object> list = new ArrayList<>();
            list.add(rpcName);
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
            logger.warn("error: " + e.getMessage());
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
    private void CallExample() throws IOException {
        ExampleArgs args = new ExampleArgs();
        args.setX(99);
        ExampleReply reply = new ExampleReply();
        reply = (ExampleReply) call("CallExample", args, reply);
        logger.info("reply.getY() = " + reply.getY());
    }

}
