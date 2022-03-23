package main;

import mr.Coordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

// start the coordinator process, which is implemented in ../mr/coordinator.java
// args[] = [input_folderPath, reduce_num]

public class MrCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(MrCoordinator.class);
    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            throw new Exception("Usage: MrSequential.java input_folderPath reduce_num");
        }
        String input_folderPath = args[0];
        String output_filePath = args[1];
        File file = new File(input_folderPath);
        File[] files = file.listFiles();
        Coordinator coordinator = new Coordinator(files, 10);
        logger.info("create coordinator success");
        coordinator.start();

        while (coordinator.isDone()){
            Thread.sleep(5000);
        }
        Thread.sleep(2000);
        logger.info("all tasks are done, coordinator exiting...");
    }
}
