package com.github.luohaha.jlitespider.core;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.github.luohaha.jlitespider.mq.MQItem;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动爬虫的点火器，用于向消息队列中添加初始内容
 * @author luoyixin
 *
 */
public class SpiderLighter {
	private ConnectionFactory factory = new ConnectionFactory();
	private Connection connection;
	private Channel sendChannel;
	private String queueName;
	private Gson gson = new Gson();
	private static final Logger logger = LoggerFactory.getLogger(SpiderLighter.class);

	public SpiderLighter(String host, int port, String queue_name) {
		super();
		factory.setHost(host);
		factory.setPort(port);
		try {
			connection = factory.newConnection();
			sendChannel = connection.createChannel();
			sendChannel.queueDeclare(queue_name, true, false, false, null);
			this.queueName = queue_name;
		}catch (Exception e){
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * 定位要发送的消息队列所在的位置，通过host:port/queue的组合来确定
	 * @param host
	 * @param port
	 * @param queue
	 * @return
	 * @throws TimeoutException
	 * @throws IOException
	 */
	public static SpiderLighter locateMQ(String host, int port, String queue) throws TimeoutException, IOException {
		return new SpiderLighter(host, port, queue);
	}
	
	public void close() throws IOException, TimeoutException {
		this.sendChannel.close();
		this.connection.close();
	}
	
	/**
	 * 添加自定义类型的数据
	 * @param key
	 * @param msg
	 * @return
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public SpiderLighter add(String key, Object msg) throws IOException, TimeoutException {
		sendChannel.basicPublish("", this.queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
				gson.toJson(new MQItem(key, msg)).getBytes());
		logger.info("add finish!");
		return this;
	}

	/**
	 * 添加url数据
	 * @param url
	 * @return
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public SpiderLighter addUrl(Object url) throws IOException, TimeoutException {
		logger.info("add new url...");
		return add("url", url);
	}

	/**
	 * 添加页面数据
	 * @param page
	 * @return
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public SpiderLighter addPage(Object page) throws IOException, TimeoutException {
		return add("page", page);
	}
	
	/**
	 * 添加解析结果数据
	 * @param result
	 * @return
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public SpiderLighter addResult(Object result) throws IOException, TimeoutException {
		return add("result", result);
	}
}
