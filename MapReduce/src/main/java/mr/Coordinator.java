package mr;

import bean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.io.File;

public class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    public static final int maxTaskTime = 10;  //seconds
    private ServerSocket serverSocket;
    private int servPort;

    private File[] files;
    private int nReduce;
    private int curWorkerId;

    private BlockQueue unIssuedMapTasks;
    private MapSet issuedMapTasks;
    private ReentrantLock issuedMapReentrantLock;

    private BlockQueue unIssuedReduceTasks;
    private MapSet issuedReduceTasks;
    private ReentrantLock issuedReduceReentrantLock;

    // task states
    private TaskState[] mapTasks;
    private TaskState[] reduceTasks;

    // states
    private boolean mapDone;
    private boolean allDone;

    public Coordinator(File[] files, int nReduce, int port) throws IOException {
        logger.info("making coordinator");
        this.servPort = port;
        this.serverSocket = new ServerSocket(this.servPort);
        this.files = files;
        this.nReduce = nReduce;
        this.curWorkerId = 0;
        this.mapTasks = new TaskState[files.length];
        this.reduceTasks = new TaskState[nReduce];
        this.unIssuedMapTasks = new BlockQueue();
        this.issuedMapTasks = new MapSet();
        this.unIssuedReduceTasks = new BlockQueue();
        this.issuedReduceTasks = new MapSet();
        this.allDone = false;
        this.mapDone = false;
        this.issuedMapReentrantLock = new ReentrantLock();
        this.issuedReduceReentrantLock = new ReentrantLock();
    }

    /**
     * Start a Coordinator, main/mrCoordinator.java calls this function.
     */
    public void start() throws Exception {
        // create output directory
        createAllDir();

        // start a thread that listens for RPCs from worker.java
        this.startServer();
        logger.info("listening started...");

        // starts a thread that abandons timeout tasks
        this.loopRemoveTimeoutMapTasks();

        // all are unissued map tasks
        // send to channel after everything else initializes
        logger.info("file count: " + files.length);
        for (int i = 0; i < files.length; i++){
            logger.info("sending " + i + "th file map task to channel");
            this.unIssuedMapTasks.PutFront(i);
        }
    }

    /**
     * Assign map task to worker, mr/Worker.java calls this function.
     * @param args contains workerId.
     * @param reply
     * @return a MapTaskReply object.
     */
    public MapTaskReply giveMapTask(MapTaskArgs args, MapTaskReply reply) {
        if (args.getWorkerId() == -1){
            // simply allocate
            reply.setWorkId(this.curWorkerId);
            this.curWorkerId++;
        }else {
            reply.setWorkId(args.getWorkerId());
        }
        logger.info("worker " + reply.getWorkId() + " asks for a map task");

        this.issuedMapReentrantLock.lock();

        if (this.mapDone){
            this.issuedMapReentrantLock.unlock();
            mapDoneProcess(reply);
        }
        if (this.unIssuedMapTasks.getSize() == 0 && this.issuedMapTasks.getCount() == 0){
            this.issuedMapReentrantLock.unlock();
            mapDoneProcess(reply);
            prepareAllReduceTasks(this);
            this.mapDone = true;
        }
        logger.info(this.unIssuedMapTasks.getSize() + " unissued map tasks, " + this.issuedMapTasks.getCount() + " issued map tasks at hand");

        // release lock to allow unissued update
        this.issuedMapReentrantLock.unlock();

        long curTime = getNowTimeSecond();
        int fileId;
        Object popData = this.unIssuedMapTasks.PopBack();
        if (popData == null){
            logger.warn("no map task yet, let worker wait...");
            fileId = -1;
        }else {
            fileId = (int)popData;
            this.issuedMapReentrantLock.lock();
            reply.setFile(this.files[fileId]);
            this.mapTasks[fileId].setBeginSecond(curTime);
            this.mapTasks[fileId].setWorkerId(reply.getWorkId());
            this.issuedMapTasks.Insert(fileId);
            this.issuedMapReentrantLock.unlock();
            logger.info("giving map task " + fileId + " on file " + reply.getFile().getName() + " at second " + formatCurTime(curTime * 1000));
        }
        reply.setFileId(fileId);
        reply.setAllDone(false);
        reply.setNReduce(this.nReduce);
        return reply;
    }

    /**
     * Prepare all reduce tasks and add them to unIssuedReduceTasks.
     * @param coordinator
     */
    private static void prepareAllReduceTasks(Coordinator coordinator) {
        for (int i = 0; i < coordinator.nReduce; i++){
            logger.info("putting " + i + "th reduce task into channel");
            coordinator.unIssuedReduceTasks.PutFront(i);
        }
    }

    /**
     * Signal completion of all map tasks.
     * @param reply
     */
    private static void mapDoneProcess(MapTaskReply reply) {
        logger.info("all map tasks complete, telling workers to switch to reduce mode");
        reply.setFileId(-1);
        reply.setAllDone(true);
    }

    /**
     * Check current time for whether the worker has timed out.
     * @param args
     * @param reply
     * @return a MapTaskJoinReply object.
     */
    public MapTaskJoinReply joinMapTask(MapTaskJoinArgs args, MapTaskJoinReply reply){
        logger.info("got join request from worker " + args.getWorkId() + " on file " + args.getFileId() + " : " + this.files[args.getFileId()].getName());

        this.issuedMapReentrantLock.lock();

        long curTime = getNowTimeSecond();
        long taskTime = this.mapTasks[args.getFileId()].getBeginSecond();
        if (!this.issuedMapTasks.Has(args.getFileId())){
            logger.info("task abandoned or does not exists, ignoring...");
            this.issuedMapReentrantLock.unlock();
            reply.setAccept(false);
        }
        if (this.mapTasks[args.getFileId()].getWorkerId() != args.getWorkId()){
            logger.info("map task belongs to worker " + this.mapTasks[args.getFileId()].getWorkerId() + " not this " + args.getWorkId() + ", ignoring...");
            this.issuedMapReentrantLock.unlock();
            reply.setAccept(false);
        }
        if (curTime - taskTime > maxTaskTime){
            logger.info("task exceeds max wait time, abandoning...");
            reply.setAccept(false);
            this.unIssuedMapTasks.PutFront(args.getFileId());
        }else {
            logger.info("task within max wait time, accepting...");
            reply.setAccept(true);
            this.issuedMapTasks.Remove(args.getFileId());
        }
        this.issuedMapReentrantLock.unlock();
        return reply;
    }

    /**
     * Assign reduce task to worker, mr/Worker.java calls this function.
     * @param args contains workId
     * @param reply
     * @return a ReduceTaskReply object.
     */
    public ReduceTaskReply GiveReduceTask(ReduceTaskArgs args, ReduceTaskReply reply){
        logger.info("worker " + args.getWorkId() + " asking for a reduce task");

        this.issuedReduceReentrantLock.lock();

        if (this.unIssuedReduceTasks.getSize() == 0 && this.issuedReduceTasks.getCount() == 0){
            logger.info("all reduce tasks complete, telling workers to terminate");
            this.issuedReduceReentrantLock.unlock();
            this.allDone = true;
            reply.setRIndex(-1);
            reply.setAllDone(true);
        }
        logger.info(this.unIssuedReduceTasks.getSize() + " unissued reduce tasks, " + this.issuedReduceTasks + " issued reduce tasks at hand");
        // release lock to allow unissued update
        this.issuedReduceReentrantLock.unlock();

        long curTime = getNowTimeSecond();
        int rIndex;
        Object popData = this.unIssuedReduceTasks.PopBack();
        if (popData == null){
            logger.warn("no reduce task yet, let worker wait...");
            rIndex = -1;
        }else {
            rIndex = (int)popData;
            this.issuedReduceReentrantLock.lock();
            this.reduceTasks[rIndex].setBeginSecond(curTime);
            this.reduceTasks[rIndex].setWorkerId(args.getWorkId());
            this.issuedReduceTasks.Insert(rIndex);
            this.issuedReduceReentrantLock.unlock();
            logger.info("giving reduce task " + rIndex + " at second " + formatCurTime(curTime * 1000));
        }
        reply.setRIndex(rIndex);
        reply.setAllDone(false);
        reply.setNReduce(this.nReduce);
        reply.setFileCount(this.files.length);
        return reply;
    }

    /**
     * Check current time for whether the worker has timed out.
     * @param args
     * @param reply
     * @return a ReduceTaskJoinReply object.
     */
    public ReduceTaskJoinReply JoinReduceTask(ReduceTaskJoinArgs args, ReduceTaskJoinReply reply){
        logger.info("got join request from worker " + args.getWorkId() + " on reduce task " + args.getRIndex());

        this.issuedReduceReentrantLock.lock();

        long curTime = getNowTimeSecond();
        long taskTime = this.reduceTasks[args.getRIndex()].getBeginSecond();
        if (!this.issuedReduceTasks.Has(args.getRIndex())){
            logger.info("task abandoned or does not exists, ignoring...");
            this.issuedReduceReentrantLock.unlock();
        }
        if (this.reduceTasks[args.getRIndex()].getWorkerId() != args.getWorkId()){
            logger.info("reduce task belongs to worker " + this.reduceTasks[args.getRIndex()].getWorkerId() + " not this " + args.getWorkId() + ", ignoring...");
            this.issuedReduceReentrantLock.unlock();
            reply.setAccept(false);
        }
        if (curTime - taskTime > maxTaskTime){
            logger.info("task exceeds max wait time, abandoning...");
            reply.setAccept(false);
            this.unIssuedReduceTasks.PutFront(args.getRIndex());
        }else {
            logger.info("task within max wait time, accepting...");
            reply.setAccept(true);
            this.issuedReduceTasks.Remove(args.getRIndex());
        }
        this.issuedReduceReentrantLock.unlock();
        return reply;
    }

    private void startServer() {
        ThreadPoolExecutor threadPool =new ThreadPoolExecutor(5, 10,
                200, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10));
        new Thread(() -> {
            while (true){
                logger.info("start rpc server...");
                try {
                    Socket socket = this.serverSocket.accept();
                    ServerService service = new ServerService(socket);
                    service.registerService(Coordinator.class);
                    threadPool.execute(service);
                } catch (IOException e){
                    System.out.println(e.getMessage());
                }
            }
        }).start();
    }

    /**
     * Starts a thread that abandons timeout tasks.
     */
    private void loopRemoveTimeoutMapTasks() {
        new Thread(() -> {
            while (true){
                try {
                    Thread.sleep(2000);
                    this.removeTimeoutTasks();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Remove map and reduce timeout tasks.
     */
    private void removeTimeoutTasks() {
        logger.info("removing timeout map tasks...");
        this.issuedMapReentrantLock.lock();
        removeTimeoutMapTasks(this.mapTasks, this.issuedMapTasks, this.unIssuedMapTasks);
        this.issuedMapReentrantLock.unlock();
        this.issuedReduceReentrantLock.lock();
        removeTimeoutReduceTasks(this.reduceTasks, this.issuedReduceTasks, this.unIssuedReduceTasks);
        this.issuedReduceReentrantLock.unlock();
    }

    /**
     * If the map task exceeds maxTaskTime, the task will be removed from issuedMapTasks MapSet and added to unIssuedMapTasks Queue.
     * @param mapTasks
     * @param issuedMapTasks
     * @param unIssuedMapTasks
     */
    private static void removeTimeoutMapTasks(TaskState[] mapTasks, MapSet issuedMapTasks, BlockQueue unIssuedMapTasks) {
        for (Map.Entry<Object, Boolean> entry: issuedMapTasks.getMapBool().entrySet()){
            long nowSecond = getNowTimeSecond();
            if (entry.getValue()){
                int key = (int) entry.getKey();
                if (nowSecond - mapTasks[key].getBeginSecond() > maxTaskTime){
                    logger.info("worker do map task " + mapTasks[key].getWorkerId() + " on file " + mapTasks[key].getFileId() + " abandoned due to timeout.");
                    issuedMapTasks.Remove(key);
                    unIssuedMapTasks.PutFront(key);
                }
            }
        }
    }

    /**
     * If the reduce task exceeds maxTaskTime, the task will be removed from issuedMapTasks MapSet and added to unIssuedMapTasks Queue.
     * @param reduceTasks
     * @param issuedReduceTasks
     * @param unIssuedReduceTasks
     */
    private static void removeTimeoutReduceTasks(TaskState[] reduceTasks, MapSet issuedReduceTasks, BlockQueue unIssuedReduceTasks) {
        for (Map.Entry<Object, Boolean> entry: issuedReduceTasks.getMapBool().entrySet()){
            long nowSecond = System.currentTimeMillis() / 1000;
            if (entry.getValue()){
                int key = (int) entry.getKey();
                if (nowSecond - reduceTasks[key].getBeginSecond() > maxTaskTime){
                    logger.info("worker do reduce task " + reduceTasks[key].getWorkerId() + " on file " + reduceTasks[key].getFileId() + " abandoned due to timeout.");
                    issuedReduceTasks.Remove(key);
                    unIssuedReduceTasks.PutFront(key);
                }
            }
        }
    }

    /**
     * Gets the current time in seconds.
     * @return
     */
    public static long getNowTimeSecond(){
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Format the current time.
     * @param curTime
     * @return yyyy年MM月dd日 HH时mm分ss秒
     */
    public static String formatCurTime(long curTime){
        SimpleDateFormat sdf = new SimpleDateFormat("", Locale.SIMPLIFIED_CHINESE);
        sdf.applyPattern("yyyy年MM月dd日 HH时mm分ss秒");
        return sdf.format(curTime);
    }

    /**
     * Check that all tasks have been completed.
     * @return whether i am done.
     */
    public boolean isDone(){
        if (this.allDone){
            logger.info("asked whether i am done, replying yes...");
        }else {
            logger.info("asked whether i am done, replying no...");
        }
        return this.allDone;
    }

    /**
     * Create output directory.
     */
    public static void createAllDir() throws Exception {
        String userDir = System.getProperty("user.dir");
        File outputDirFile = new File(userDir + File.separator + "output");
        File tempDirFile = new File(userDir + File.separator + "output" + File.separator + "temp");
        createDir(outputDirFile);
        createDir(tempDirFile);
    }

    public static void createDir(File file) throws Exception {
        boolean isDirExisted = file.exists();
        if (isDirExisted){
            logger.warn(file.getAbsoluteFile() + " is existed ...");
        }else {
            isDirExisted = file.mkdirs();
            if (isDirExisted){
                logger.info("Create " + file.getAbsoluteFile() + " successfully!");
            }else {
                throw new Exception("Disable to make the folder,please check the disk is full or not.");
            }
        }
    }

    /**
     * An example RPC handler.
     * The RPC argument and reply types are defined in rpc.go.
     * @param args
     * @param reply
     */
    public void CallExample(ExampleArgs args, ExampleReply reply){
        logger.info("get call from worker, set y = x + 1");
        reply.setY(args.getX() + 1);
    }

}
