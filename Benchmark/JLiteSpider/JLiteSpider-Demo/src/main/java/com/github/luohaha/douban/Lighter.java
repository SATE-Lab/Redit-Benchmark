package com.github.luohaha.douban;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import com.github.luohaha.jlitespider.core.SpiderLighter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lighter {
	private static final Logger logger = LoggerFactory.getLogger(Lighter.class);
	private static String[] urls = new String[]{"https://movie.douban.com/chart", "https://movie.douban.com/top250"};
	public static void main(String[] args) {
		try {
			String rabbitmqIp = args[0];
			// 初始化指定的消息队列，往消息队列中添加入口url
			for (String url : urls){
				logger.info("Lighter add url to mq-1:" + rabbitmqIp + " : " +  url);
				SpiderLighter.locateMQ(rabbitmqIp, 5672, "mq-1")
						.addUrl(url)
						.close();
				Thread.sleep(3000);
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
