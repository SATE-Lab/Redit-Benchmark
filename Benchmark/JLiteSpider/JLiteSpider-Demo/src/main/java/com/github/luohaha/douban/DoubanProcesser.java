package com.github.luohaha.douban;

import java.io.IOException;
import java.util.Map;
import java.util.List;

import com.github.luohaha.jlitespider.core.Spider;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.github.luohaha.jlitespider.core.MessageQueue;
import com.github.luohaha.jlitespider.core.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.xsoup.Xsoup;

public class DoubanProcesser implements Processor {
	private static final Logger logger = LoggerFactory.getLogger(DoubanProcesser.class);

	public void process(Object page, Map<String, MessageQueue> mQueue) throws IOException {
		// TODO Auto-generated method stub
		// 解析出想要接下去访问的url地址
		logger.info("Parse out the next url address...");
		Document document = Jsoup.parse((String) page);
		logger.info("page: \n" + document);
		List<String> urls = Xsoup.compile("//tbody/tr/td[2]/div/a/@href").evaluate(document).list();
		urls.forEach(url -> {
			try {
				// 将获取到的url地址，发往队列，并打上result标签
				logger.info("Send the obtained url address to the queue and tag the result");
				mQueue.get("mq-1").sendResult(url);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
	}

}
