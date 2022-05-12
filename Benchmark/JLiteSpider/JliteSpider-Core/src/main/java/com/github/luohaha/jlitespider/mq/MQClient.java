package com.github.luohaha.jlitespider.mq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MQClient {
	protected ConnectionFactory factory = new ConnectionFactory();
	protected Connection connection;
	protected Channel channel;
	protected String queueName;
	
	public MQClient(String host, int port, String queue_name) throws IOException, TimeoutException {
		factory.setHost(host);
		factory.setPort(port);
		this.queueName = queue_name;
		connection = factory.newConnection();
		channel = connection.createChannel();
		channel.queueDeclare(queue_name, true, false, false, null);
	}
}
