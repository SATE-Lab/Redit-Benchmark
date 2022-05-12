package com.github.luohaha.jlitespider.setting;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.google.gson.Gson;

public class SettingReader {
	/**
	 * 读取配置文件信息，返回配置文件对象
	 * @param file
	 * @return
	 */
	public static SettingObject read(String file) {
		Gson gson = new Gson();
		SettingObject object = gson.fromJson(readFile(file), SettingObject.class);
		return object;
	}
	
	/**
	 * 读取文件中的内容，返回字符串
	 * @param filename
	 * @return
	 */
	public static String readFile(String filename) {
		String ret = "";
		File file = new File(filename);
        BufferedReader reader = null;  
        try {  
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            StringBuffer buffer = new StringBuffer();
            while ((tempString = reader.readLine()) != null) {  
            		buffer.append(tempString);
            }
            ret = buffer.toString();
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }
        return ret;
	}
}
