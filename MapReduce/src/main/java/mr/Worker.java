package mr;

import bean.*;
import mrapps.WordCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class Worker {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private ArrayList<KeyValue> intermediate;
    // false on map, true on reduce
    private boolean mapOrReduce;
    // must exit if true
    public boolean allDone;
    private int workerId;

    public Worker() {
        logger.info("making worker");
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
    private MapTaskReply askMapTask() {
        MapTaskArgs args = new MapTaskArgs();
        args.setWorkerId(this.workerId);
        MapTaskReply reply = new MapTaskReply();
        logger.info("requesting for map task...");
        call("Coordinator.GiveMapTask", args, reply);
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

    private void writeToFiles(int fileId, int nReduce, ArrayList<KeyValue> intermediate) throws Exception {
        ArrayList<ArrayList<KeyValue>> keyValues = new ArrayList<ArrayList<KeyValue>>(nReduce);
        for (int i = 0; i < nReduce; i++){
            keyValues.add(i, new ArrayList<KeyValue>(0));
        }
        for (KeyValue kv : intermediate){
            int index = keyReduceIndex(kv.getKey(), nReduce);
            keyValues.get(index).add(kv);
        }
        for (int i = 0; i < nReduce; i++){
            File file = createWriteFile(i);
            // TODO
        }
    }

    /**
     * Choose the reduce task number for each KeyValue emitted by Map.
     * @param key
     * @param nReduce
     * @return
     */
    private int keyReduceIndex(String key, int nReduce) {
        // TODO
        return 0;
    }

    private void joinMapTask(int fileId) {
        // TODO
    }


    private ReduceTaskReply askReduceTask() {
        return null;
    }

    private void executeReduce(ReduceTaskReply reply) {
    }

    private boolean call(String rpcName, TaskArgs args, TaskReply reply){
        // TODO
        return false;
    }

    /**
     * Create temp write files.
     * @param id
     * @return File object.
     * @throws IOException
     */
    private File createWriteFile(int id) throws Exception {
        String userDir = System.getProperty("user.dir");
        File tempDirFile = new File(userDir + File.separator + "output" + File.separator + "temp");
        if (tempDirFile.isDirectory()){
            File writeFile = new File(tempDirFile.getAbsoluteFile() + File.separator + "mr-temp-" + id);
            if(!writeFile.exists()){
                writeFile.createNewFile();
            }
            return writeFile;
        }else {
            throw new Exception(tempDirFile + " is not exist, can not write.");
        }
    }


}
