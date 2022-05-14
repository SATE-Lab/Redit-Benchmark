package com.github.luohaha.jlitespider.mq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.google.gson.Gson;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQRecver extends MQClient {
	private QueueingConsumer consumer;
	private Gson gson = new Gson();
	private static final Logger logger = LoggerFactory.getLogger(MQRecver.class);
	
	public MQRecver(String host, int port, String queue_name, int qos) throws IOException, TimeoutException {
		super(host, port, queue_name);
		channel.basicQos(qos);
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(queue_name, false, consumer);
	}
	
	public MQItem recv() throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		logger.info("MQRecver recv...");
		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		MQItem item = gson.fromJson(new String(delivery.getBody()), MQItem.class);
	    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	    return item;
	}
}
