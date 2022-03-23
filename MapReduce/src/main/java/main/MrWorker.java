package main;

import mr.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// start a worker process, which is implemented in ../mr/worker.java.
// typically there will be multiple worker processes, talking to one coordinator.

public class MrWorker {
    private static final Logger logger = LoggerFactory.getLogger(MrCoordinator.class);
    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new Exception("Usage: MrWorker.java");
        }
        Worker worker = new Worker();
        while (!worker.allDone){
            worker.process();
        }
        logger.info("no more tasks, all done, worker exiting...");
    }
}
