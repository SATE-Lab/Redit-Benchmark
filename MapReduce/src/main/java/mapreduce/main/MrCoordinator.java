package mapreduce.main;

import mapreduce.mr.Coordinator;
import java.io.File;

// start the coordinator process, which is implemented in ../mapreduce.mr/coordinator.java
// args[] = [input_folderPath, reduce_num]

public class MrCoordinator {
    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            throw new Exception("Usage: MrSequential.java input_folderPath reduce_num");
        }
        String input_folderPath = args[0];
        File file = new File(input_folderPath);
        File[] files = file.listFiles();
        Coordinator coordinator = new Coordinator(files, 10, 12000);
        System.out.println("MrCoordinator: " + "create coordinator success");
        coordinator.start();

        while (coordinator.isDone()){
            Thread.sleep(5000);
        }
        Thread.sleep(2000);
        System.out.println("MrCoordinator: " + "all tasks are done, coordinator exiting...");
    }
}
