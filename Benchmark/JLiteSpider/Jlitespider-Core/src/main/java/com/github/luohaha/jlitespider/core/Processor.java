package com.github.luohaha.jlitespider.core;

import java.io.IOException;
import java.util.Map;

/**
 * author: Yixin Luo
 * 2016/3/3
 * 
 * 解析器的接口。解析器用于提取关键信息，使用jsoup或正则表达式等完成任务。
 * 
 * **/
public interface Processor{
	/**
	 * 处理下载下来的页面源代码
	 * @param page
	 * 消息队列推送过来的页面源代码数据消息
	 * @param mQueue
	 * 提供把消息发送到各个消息队列的方法
	 * @throws IOException
	 */
	public void process(Object page, Map<String, MessageQueue> mQueue) throws IOException;
}

