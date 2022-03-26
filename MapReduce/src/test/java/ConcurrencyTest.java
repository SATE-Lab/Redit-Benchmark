import mr.Coordinator;
import mr.Worker;
import java.io.File;

public class ConcurrencyTest {

    public static void main(String[] args) throws Exception{
        String input_folderPath = "D:\\product\\Redit\\Redit-push\\Redit-Benchmark\\MapReduce\\src\\main\\java\\files";
        File file = new File(input_folderPath);
        File[] files = file.listFiles();
        Coordinator coordinator = new Coordinator(files, 10, 12000);
        System.out.println("ConcurrencyTest : create coordinator success !!!");

        new Thread( () ->  {
            try {
                coordinator.start();
                while (!coordinator.isDone()){
                    Thread.sleep(2000);
                }
                System.out.println("ConcurrencyTest: " + "all tasks are done, coordinator exiting...");
            } catch (Exception e){
                e.printStackTrace();
            }
        } ).start();

        Thread.sleep(5000);
        int workerNum = 5;
        for (int i = 0; i < workerNum; i++){
            startWorker();
        }
        Thread.sleep(3000);

    }

    private static void startWorker(){
        new Thread( () ->  {
            try {
                Worker worker = new Worker();
                while (!worker.allDone){
                    worker.process();
                }
                System.out.println("ConcurrencyTest: " + "all tasks are done, worker " + worker.workerId + " exiting...");
            } catch (Exception e){
                e.printStackTrace();
            }
        } ).start();
    }
}
