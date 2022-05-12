package com.github.luohaha.jlitespider.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.github.luohaha.jlitespider.exception.SpiderLackOfMethodException;
import com.github.luohaha.jlitespider.exception.SpiderSettingFileException;
import com.github.luohaha.jlitespider.mq.MQItem;
import com.github.luohaha.jlitespider.mq.MQRecver;
import com.github.luohaha.jlitespider.mq.MQSender;
import com.github.luohaha.jlitespider.setting.MqObject;
import com.github.luohaha.jlitespider.setting.SettingObject;
import com.github.luohaha.jlitespider.setting.SettingReader;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;


/**
 * author:Yixin Luo
 * 2016/3/3
 * 
 * 爬虫组装工厂
 * **/
public class Spider {

	/**
	 * 处理函数接口
	 */
	private Downloader downloader;
	private Processor processor;
	private Saver saver;
	private Freeman freeman;
	//配置文件位置
	private String settingFile;
	//配置文件信息
	private SettingObject settingObject;
	//读取和写入的队列
	private Map<String, MqObject> mqMap = new HashMap<>();
	private Map<String, MessageQueue> sendtoMap = new HashMap<>();
	private Map<String, MQRecver> recvfromMap = new HashMap<>();
	//log
	private Logger logger = Logger.getLogger("spider");
	
	public static Spider create() {
		return new Spider();
	}
	
	public Spider setDownloader(Downloader d) {
		this.downloader = d;
		return this;
	}
	
	public Spider setProcessor(Processor p) {
		this.processor = p;
		return this;
	}
	
	public Spider setSaver(Saver s) {
		this.saver = s;
		return this;
	}
	
	public Spider setFreeman(Freeman freeman) {
		this.freeman = freeman;
		return this;
	}
	
	public Spider setSettingFile(String filename) {
		this.settingFile = filename;
		return this;
	}

	/**
	 * 读取配置文件
	 * @throws TimeoutException 
	 * @throws IOException 
	 * @throws SpiderSettingFileException 
	 */
	private void readSetting() throws IOException, TimeoutException, SpiderSettingFileException {
		if (this.settingFile == null) {
			throw new FileNotFoundException();
		} else {
			this.settingObject = SettingReader.read(this.settingFile);
			for (MqObject each : this.settingObject.getMq()) {
				this.mqMap.put(each.getName(), each);
			}
			for (String each : this.settingObject.getSendto()) {
				MqObject object = this.mqMap.get(each);
				if (object == null) throw new SpiderSettingFileException();
				MQSender sender = new MQSender(object.getHost(), object.getPort(), object.getQueue());
				this.sendtoMap.put(object.getName(), sender);
			}
			for (String each : this.settingObject.getRecvfrom()) {
				MqObject object = this.mqMap.get(each);
				if (object == null) throw new SpiderSettingFileException();
				MQRecver recver = new MQRecver(object.getHost(), object.getPort(), object.getQueue(), object.getQos());
				this.recvfromMap.put(object.getName(), recver);
			}
		}
	}
	
	/**
	 * 获取可发送的消息队列
	 * @return
	 */
	public Map<String, MessageQueue> getSendToMQ() {
		return this.sendtoMap;
	}
	
	/*开始下载和解析*/
	public void begin() throws IOException, TimeoutException, ShutdownSignalException, 
	                    ConsumerCancelledException, InterruptedException, SpiderSettingFileException{
		readSetting();
		logger.info("worker [" + this.settingObject.getWorkerid() + "] start...");
		for (Entry<String, MQRecver> recv : this.recvfromMap.entrySet()) {
			new Thread(new RecvThread(this, recv.getKey(), recv.getValue(), this.sendtoMap)).start();
		}
	}
	
	/**
	 * 对于每一个要接收消息的消息队列，都启动一个线程来处理
	 * @author luoyixin
	 *
	 */
	class RecvThread implements Runnable {
		private Spider spider;
		private String mqname;
		private MQRecver recver;
		private Map<String, MessageQueue> senderMap;
		
		public RecvThread(Spider spider, String mqname, MQRecver recver, Map<String, MessageQueue> senderMap) {
			super();
			this.spider = spider;
			this.mqname = mqname;
			this.recver = recver;
			this.senderMap = sendtoMap;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				MQItem item;
				try {
					item = recver.recv();
					logger.info("recv message from MQ[" + this.mqname + "]: " + item.getKey());
					switch (item.getKey()) {
					case "url":
						if (spider.downloader == null) throw new SpiderLackOfMethodException();
						spider.downloader.download(item.getValue(), this.senderMap);
						logger.info("downloader finish!");
						break;
					case "page":
						if (spider.processor == null) throw new SpiderLackOfMethodException();
						spider.processor.process(item.getValue(), this.senderMap);
						logger.info("processor finish!");
						break;
					case "result":
						if (spider.saver == null) throw new SpiderLackOfMethodException();
						spider.saver.save(item.getValue(), this.senderMap);
						logger.info("saver finish!");
						break;
					default:
						if (spider.freeman == null) throw new SpiderLackOfMethodException();
						spider.freeman.doSomeThing(item.getKey(), item.getValue(), this.senderMap);
						logger.info("freeman finish!");
						break;
					}
				} catch (ShutdownSignalException | ConsumerCancelledException | IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SpiderLackOfMethodException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		
	}
}

