package com.github.luohaha.jlitespider.extension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.Header;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * author: Yixin Luo 2016/3/3
 * 
 * 简单的对请求的url操作进行封装， 返回对应的html字符串
 * 
 **/

public class Network {
	/* user agent */
	private String agent = null;
	/* 设置cookie */
	private String cookie = null;
	/* 传输超时 */
	private int timeout = 1000;
	/* 设置代理 */
	private String proxy = null;

	public static Network create() {
		return new Network();
	}

	/**
	 * 设置下载传输参数
	 **/
	public Network setUserAgent(String s) {
		this.agent = s;
		return this;
	}

	public Network setCookie(String c) {
		this.cookie = c;
		return this;
	}

	public Network setTimeout(int t) {
		this.timeout = t;
		return this;
	}

	public Network setProxy(String p) {
		this.proxy = p;
		return this;
	}

	/**
	 * 下载，并返回string
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 **/
	public String downloader(String url) throws ClientProtocolException, IOException {
		String res = "";
		Request rq = Request.Get(url).connectTimeout(this.timeout);
		if (this.agent != null)
			rq = rq.userAgent(this.agent);
		if (this.cookie != null)
			rq = rq.addHeader("Cookie", this.cookie);
		if (this.proxy != null)
			rq = rq.viaProxy(this.proxy);
		res = rq.execute().returnContent().asString();
		return res;
	}

}
