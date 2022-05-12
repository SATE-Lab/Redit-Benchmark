package com.github.luohaha.jlitespider.core;

import java.io.IOException;
import java.util.Map;


/**
 * author : Yixin Luo
 * 2016/3/4
 * 
 * 数据持久化的接口
 * **/
public interface Saver {
	/**
	 * 处理最终解析得到的结果
	 * @param result 
	 * 消息队列推送过来的结果消息
	 * @param mQueue 
	 * 提供把消息发送到各个消息队列的方法
	 * @throws IOException
	 */
	public void save(Object result, Map<String, MessageQueue> mQueue) throws IOException;
}
