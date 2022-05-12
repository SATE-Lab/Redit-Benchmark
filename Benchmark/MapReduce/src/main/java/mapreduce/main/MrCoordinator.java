package mapreduce.main;
import mapreduce.mr.Coordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// start the coordinator process, which is implemented in ../mapreduce.mr/coordinator.java

public class MrCoordinator {
    public static final Logger logger = LoggerFactory.getLogger(MrCoordinator.class);
    public static void main(String[] args) throws Exception {
        if (args.length > 1){
            throw new Exception("Usage: MrCoordinator.java");
        }
        Coordinator coordinator = new Coordinator(10, 12000);
        logger.info("MrCoordinator: " + "create coordinator success");
        coordinator.start();

        while (!coordinator.isDone()){
            Thread.sleep(5000);
        }
        Thread.sleep(2000);
        logger.info("MrCoordinator: " + "all tasks are done, coordinator exiting...");
    }
}
