package mr;

import bean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.io.File;

public class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    public static final int maxTaskTime = 10;  //seconds
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


    /**
     * Create a Coordinator, main/mrCoordinator.java calls this function.
     * @param files is a collection of all input files.
     * @param nReduce is the number of reduce tasks to use.
     * @return a created Coordinator object.
     */
    public static Coordinator makeCoordinator(File[] files, int nReduce){
        Coordinator coordinator = new Coordinator();
        logger.info("making coordinator");

        // initialization
        coordinator.files = files;
        coordinator.nReduce = nReduce;
        coordinator.curWorkerId = 0;
        coordinator.mapTasks = new TaskState[files.length];
        coordinator.reduceTasks = new TaskState[nReduce];
        coordinator.unIssuedMapTasks = new BlockQueue();
        coordinator.issuedMapTasks = new MapSet();
        coordinator.unIssuedReduceTasks = new BlockQueue();
        coordinator.issuedReduceTasks = new MapSet();
        coordinator.allDone = false;
        coordinator.mapDone = false;

        // start a thread that listens for RPCs from worker.java
        startServer(coordinator);
        logger.info("listening started...");

        // starts a thread that abandons timeout tasks
        loopRemoveTimeoutMapTasks(coordinator);

        // all are unissued map tasks
        // send to channel after everything else initializes
        logger.info("file count: " + files.length);
        for (int i = 0; i < files.length; i++){
            logger.info("sending " + i + "th file map task to channel");
            coordinator.unIssuedMapTasks.PutFront(i);
        }
        return coordinator;
    }

    /**
     * Assign map task to worker, mr/Worker.java calls this function.
     * @param coordinator
     * @param args contains workerId.
     * @return a MapTaskReply object.
     */
    public static MapTaskReply giveMapTask(Coordinator coordinator, MapTaskArgs args) {
        MapTaskReply reply = new MapTaskReply();
        if (args.getWorkerId() == -1){
            // simply allocate
            reply.setWorkId(coordinator.curWorkerId);
            coordinator.curWorkerId++;
        }else {
            reply.setWorkId(args.getWorkerId());
        }
        logger.info("worker " + reply.getWorkId() + " asks for a map task");

        coordinator.issuedMapReentrantLock.lock();

        if (coordinator.mapDone){
            coordinator.issuedMapReentrantLock.unlock();
            mapDoneProcess(reply);
        }
        if (coordinator.unIssuedMapTasks.getSize() == 0 && coordinator.issuedMapTasks.getCount() == 0){
            coordinator.issuedMapReentrantLock.unlock();
            mapDoneProcess(reply);
            prepareAllReduceTasks(coordinator);
            coordinator.mapDone = true;
        }
        logger.info(coordinator.unIssuedMapTasks.getSize() + " unissued map tasks, " + coordinator.issuedMapTasks.getCount() + " issued map tasks at hand");

        // release lock to allow unissued update
        coordinator.issuedMapReentrantLock.unlock();

        long curTime = getNowTimeSecond();
        int fileId;
        Object popData = coordinator.unIssuedMapTasks.PopBack();
        if (popData == null){
            logger.warn("no map task yet, let worker wait...");
            fileId = -1;
        }else {
            fileId = (int)popData;
            coordinator.issuedMapReentrantLock.lock();
            reply.setFile(coordinator.files[fileId]);
            coordinator.mapTasks[fileId].setBeginSecond(curTime);
            coordinator.mapTasks[fileId].setWorkerId(reply.getWorkId());
            coordinator.issuedMapTasks.Insert(fileId);
            coordinator.issuedMapReentrantLock.unlock();
            logger.info("giving map task " + fileId + " on file " + reply.getFile().getName() + " at second " + formatCurTime(curTime * 1000));
        }
        reply.setFileId(fileId);
        reply.setAllDone(false);
        reply.setnReduce(coordinator.nReduce);
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
     * @param coordinator
     * @param args
     * @return a MapTaskJoinReply object.
     */
    public static MapTaskJoinReply joinMapTask(Coordinator coordinator, MapTaskJoinArgs args){
        MapTaskJoinReply reply = new MapTaskJoinReply();
        logger.info("got join request from worker " + args.getWorkId() + " on file " + args.getFileId() + " : " + coordinator.files[args.getFileId()].getName());

        coordinator.issuedMapReentrantLock.lock();

        long curTime = getNowTimeSecond();
        long taskTime = coordinator.mapTasks[args.getFileId()].getBeginSecond();
        if (!coordinator.issuedMapTasks.Has(args.getFileId())){
            logger.info("task abandoned or does not exists, ignoring...");
            coordinator.issuedMapReentrantLock.unlock();
            reply.setAccept(false);
        }
        if (coordinator.mapTasks[args.getFileId()].getWorkerId() != args.getWorkId()){
            logger.info("map task belongs to worker " + coordinator.mapTasks[args.getFileId()].getWorkerId() + " not this " + args.getWorkId() + ", ignoring...");
            coordinator.issuedMapReentrantLock.unlock();
            reply.setAccept(false);
        }
        if (curTime - taskTime > maxTaskTime){
            logger.info("task exceeds max wait time, abandoning...");
            reply.setAccept(false);
            coordinator.unIssuedMapTasks.PutFront(args.getFileId());
        }else {
            logger.info("task within max wait time, accepting...");
            reply.setAccept(true);
            coordinator.issuedMapTasks.Remove(args.getFileId());
        }
        coordinator.issuedMapReentrantLock.unlock();
        return reply;
    }

    public static void GiveReduceTask(){
        // TODO
    }

    public static void JoinReduceTask(){
        // TODO
    }

    private static void startServer(Coordinator coordinator) {
        new Thread(() -> {
            while (true){
                logger.info("call server()");
                // TODO
            }
        }).start();
    }

    /**
     * Starts a thread that abandons timeout tasks.
     * @param coordinator
     */
    private static void loopRemoveTimeoutMapTasks(Coordinator coordinator) {
        new Thread(() -> {
            while (true){
                try {
                    Thread.sleep(2000);
                    removeTimeoutTasks(coordinator);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Remove map and reduce timeout tasks.
     * @param coordinator
     */
    private static void removeTimeoutTasks(Coordinator coordinator) {
        logger.info("removing timeout map tasks...");
        coordinator.issuedMapReentrantLock.lock();
        removeTimeoutMapTasks(coordinator.mapTasks, coordinator.issuedMapTasks, coordinator.unIssuedMapTasks);
        coordinator.issuedMapReentrantLock.unlock();
        coordinator.issuedReduceReentrantLock.lock();
        removeTimeoutReduceTasks(coordinator.reduceTasks, coordinator.issuedReduceTasks, coordinator.unIssuedReduceTasks);
        coordinator.issuedReduceReentrantLock.unlock();
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
     * @param coordinator
     * @return whether i am done.
     */
    public static boolean isDone(Coordinator coordinator){
        if (coordinator.allDone){
            logger.info("asked whether i am done, replying yes...");
        }else {
            logger.info("asked whether i am done, replying no...");
        }
        return coordinator.allDone;
    }
}
