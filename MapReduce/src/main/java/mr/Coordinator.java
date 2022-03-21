package mr;

import bean.BlockQueue;
import bean.MapSet;
import bean.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.io.File;


public class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
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

        // TODO

        return coordinator;
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
        removeTimeoutMapTasks(coordinator.mapTasks, coordinator.unIssuedMapTasks);
        coordinator.issuedMapReentrantLock.unlock();
        coordinator.issuedReduceReentrantLock.lock();
        removeTimeoutReduceTasks(coordinator.reduceTasks, coordinator.unIssuedReduceTasks);
        coordinator.issuedReduceReentrantLock.unlock();
    }

    private static void removeTimeoutMapTasks(TaskState[] mapTasks, BlockQueue unIssuedMapTasks) {
        // TODO
    }

    private static void removeTimeoutReduceTasks(TaskState[] reduceTasks, BlockQueue unIssuedReduceTasks) {
        // TODO
    }


    private static void startServer(Coordinator coordinator) {
        logger.info("call server()");
        // TODO
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
