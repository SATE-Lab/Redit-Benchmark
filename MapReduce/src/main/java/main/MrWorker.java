package main;

import mr.Worker;

// start a worker process, which is implemented in ../mr/worker.java.
// typically there will be multiple worker processes, talking to one coordinator.

public class MrWorker {
    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new Exception("Usage: MrWorker.java");
        }
        Worker worker = new Worker();
        while (!worker.allDone){
            worker.process();
        }
        System.out.println("MrWorker: " + "no more tasks, all done, worker exiting...");
    }
}
