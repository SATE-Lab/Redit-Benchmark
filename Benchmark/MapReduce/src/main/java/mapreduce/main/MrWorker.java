package mapreduce.main;

import mapreduce.mr.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// start a worker process, which is implemented in ../mapreduce.mr/worker.java.
// typically there will be multiple worker processes, talking to one coordinator.

public class MrWorker {
    public static final Logger logger = LoggerFactory.getLogger(MrWorker.class);
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new Exception("Usage: MrWorker.java coordinatorIp");
        }
        String coordinatorIp = args[0];
        Worker worker = new Worker(coordinatorIp);
        while (!worker.allDone){
            worker.process();
        }
        logger.info("MrWorker: " + "no more tasks, all done, worker exiting...");
    }
}
