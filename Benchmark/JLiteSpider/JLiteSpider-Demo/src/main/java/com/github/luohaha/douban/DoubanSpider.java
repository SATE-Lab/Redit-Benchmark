package com.github.luohaha.douban;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeoutException;

import com.github.luohaha.jlitespider.core.Spider;
import com.github.luohaha.jlitespider.exception.SpiderSettingFileException;
import com.github.luohaha.jlitespider.extension.AsyncNetwork;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubanSpider {
	private static final Logger logger = LoggerFactory.getLogger(DoubanSpider.class);
	public static void main(String[] args) {
		try {
			String rabbitmqIp = args[0];
			// 初始化下载器
			logger.info("Initialize the downloader...");
			AsyncNetwork asyncNetwork = new AsyncNetwork();
			asyncNetwork.begin();

			// 启动下载进程
			logger.info("Start the download process...");
			Spider.create().setDownloader(new DoubanDownloader(asyncNetwork))
				  .setProcessor(new DoubanProcesser())
				  .setSaver(new DoubanSaver(asyncNetwork))
				  .setSettingFile("douban.json", rabbitmqIp)
				  .begin();
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpiderSettingFileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
