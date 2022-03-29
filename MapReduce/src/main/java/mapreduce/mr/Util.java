package mapreduce.mr;

import java.util.ArrayList;
import java.util.Arrays;

public class Util {
    public static ArrayList<String> contentSplit(StringBuffer sb){
        String[] splitTag = new String[]{", ", ". ", ": ", "; "};
        char[] splitTag2 = new char[]{' ', '?', '"'};
        ArrayList<String> rs = new ArrayList<>();
        int start = 0;
        int end = 0;
        for (int i = 0; i < sb.length(); i ++){
            System.out.println(sb.substring(i, i + 2));
            if (Arrays.asList(splitTag).contains(sb.substring(i, i + 2))){
                System.out.println("match 1");
                end = i - 1;
                if (end > start){
                    String word = sb.substring(start, end);
                    rs.add(word);
                    start = i + 2;
                    i++;
                }
            }
            if (Arrays.asList(splitTag2).contains(sb.charAt(i))){
                System.out.println("match 2");
                end = i - 1;
                if (end > start){
                    String word = sb.substring(start, end);
                    rs.add(word);
                    start = i + 1;
                }
            }
        }
        return rs;
    }
}
