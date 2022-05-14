package com.github.luohaha.jlitespider.mq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.github.luohaha.jlitespider.core.Spider;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQClient {
	protected ConnectionFactory factory = new ConnectionFactory();
	protected Connection connection;
	protected Channel channel;
	protected String queueName;
	private static final Logger logger = LoggerFactory.getLogger(MQClient.class);
	
	public MQClient(String host, int port, String queue_name) throws IOException, TimeoutException {
		factory.setHost(host);
		factory.setPort(port);
		this.queueName = queue_name;
		connection = factory.newConnection();
		logger.info("MQClient new Connection...");
		channel = connection.createChannel();
		logger.info("MQClient create Channel...");
		channel.queueDeclare(queue_name, true, false, false, null);
	}
}
