package com.github.luohaha.jlitespider.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.luohaha.jlitespider.mq.MQItem;

/**
 * author: Yixin Luo
 * 2016/3/4
 * 
 * 下载器的接口
 * **/
public interface Downloader {
	/**
	 * 下载url所指定的页面。
	 * @param url 
	 * 收到的由消息队列传过来的消息
	 * @param mQueue 
	 * 提供把消息发送到各个消息队列的方法
	 * @throws IOException
	 */
	public void download(Object url, Map<String, MessageQueue> mQueue) throws IOException;
}
