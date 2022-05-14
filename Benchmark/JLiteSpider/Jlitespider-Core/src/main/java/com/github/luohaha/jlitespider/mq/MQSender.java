package com.github.luohaha.jlitespider.mq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.github.luohaha.jlitespider.core.MessageQueue;
import com.google.gson.Gson;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSender extends MQClient implements MessageQueue {
	private Gson gson = new Gson();
	private static final Logger logger = LoggerFactory.getLogger(MQSender.class);

	public MQSender(String host, int port, String queue_name) throws IOException, TimeoutException {
		super(host, port, queue_name);
	}
	
	private void send(MQItem item) throws IOException {
		channel.basicPublish("", this.queueName, 
	            MessageProperties.PERSISTENT_TEXT_PLAIN,
	            gson.toJson(item).getBytes());
	}
	
	public void sendUrl(Object url) throws IOException {
		logger.info("MQSender send url...");
		send(new MQItem("url", url));
	}
	
	public void sendPage(Object page) throws IOException {
		logger.info("MQSender send page...");
		send(new MQItem("page", page));
	}
	
	public void sendResult(Object result) throws IOException {
		logger.info("MQSender send result...");
		send(new MQItem("result", result));
	}

	public void send(String key, Object msg) throws IOException {
		send(new MQItem(key, msg));
	}
}
