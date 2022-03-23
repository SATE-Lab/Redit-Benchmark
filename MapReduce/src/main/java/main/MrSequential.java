package main;

import bean.KeyValue;
import mrapps.WordCount;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

// simple sequential MapReduce.
// args[] = [input_folderPath, output_filePath]

public class MrSequential {
    private static ArrayList<KeyValue> keyValues = new ArrayList<>();
    private static ArrayList<KeyValue> intermediate = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            throw new Exception("Usage: MrSequential.java input_folderPath output_filePath");
        }
        String folderPath = args[0];
        File file = new File(folderPath);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++){
            FileReader fr = new FileReader(files[i]);
            BufferedReader br = new BufferedReader(fr);
            String line;
            StringBuffer content = new StringBuffer();
            while((line = br.readLine()) != null){
                content.append(line);
            }
            keyValues = WordCount.map(content);
            intermediate.addAll(keyValues);
        }
        Collections.sort(intermediate);

        File inputFile = new File(args[1]);
        if(!inputFile.exists()){
            inputFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(inputFile,true);
        int i = 0, j;
        while (i < intermediate.size()){
            j = i + 1;
            while (j < intermediate.size() && intermediate.get(j).getKey().equals(intermediate.get(i).getKey())){
                j++;
            }
            ArrayList<String> values = new ArrayList<>();
            for (int k = i; k < j; k++){
                values.add(intermediate.get(k).getValue());
            }
            int count = WordCount.reduce(values);
            String data = intermediate.get(i).getKey() + " " + count + "\r\n";
            fileWriter.write(data);
            i = j;
        }
        fileWriter.close();
    }

}
