package com.github.luohaha.jlitespider.core;

import java.io.IOException;
import java.util.Map;

/**
 * 自定义的，自由的处理接口
 * @author luoyixin
 *
 */
public interface Freeman {
	/**
	 * 自定义的处理函数
	 * @param key
	 * key为自定义的消息标记
	 * @param msg
	 * 消息队列推送的消息
	 * @param mQueue
	 * 提供把消息发送到各个消息队列的方法
	 * @throws IOException
	 */
	public void doSomeThing(String key, Object msg, Map<String, MessageQueue> mQueue) throws IOException;
}
