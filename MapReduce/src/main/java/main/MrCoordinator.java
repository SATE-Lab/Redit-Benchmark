package main;

// start the coordinator process, which is implemented in ../mr/coordinator.java
// args[] = [input_folderPath, output_filePath]
import mr.*;
import java.io.File;

public class MrCoordinator {
    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            throw new Exception("Usage: MrSequential.java input_folderPath output_filePath");
        }
        String folderPath = args[0];
        File file = new File(folderPath);
        File[] files = file.listFiles();
        Coordinator.makeCoordinator(files, 10);
    }
}
