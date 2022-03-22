package mr;

import bean.BlockQueue;
import bean.MapSet;
import bean.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            long nowSecond = System.currentTimeMillis() / 1000;
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
