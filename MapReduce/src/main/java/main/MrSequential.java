package main;

import mr.*;

import java.io.*;
import java.util.HashMap;

public class MrSequential {

    // for sorting by key.
    private static HashMap<String,Integer> keyValue = new HashMap<>();

    public static void main(String[] args) throws IOException {
        String folderPath = args[0];
        File file = new File(folderPath);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++){
            FileReader fr = new FileReader(files[i]);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null){
                //process the line
                System.out.println(line);
            }

        }
    }


}
