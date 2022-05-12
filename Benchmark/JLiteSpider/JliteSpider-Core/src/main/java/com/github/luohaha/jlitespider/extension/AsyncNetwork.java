package com.github.luohaha.jlitespider.extension;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.protocol.HttpContext;

public class AsyncNetwork {
	public interface DownloadCallback {
		public void onReceived(String result, String url);
		public void onFailed(Exception exception, String url);
	}
	
	class Url {
		String url;
		String result = "";
		DownloadCallback callback;
		public Url(String url, DownloadCallback callback) {
			super();
			this.url = url;
			this.callback = callback;
		}
		
	}

	/**
	 * 获取网页信息
	 * @author luoyixin
	 *
	 */
	class MyResponseConsumer extends AsyncCharConsumer<Boolean> {
		
		private Url url;
		
		public MyResponseConsumer(Url url) {
			// TODO Auto-generated constructor stub
			this.url = url;
		}

		@Override
		protected void onResponseReceived(final HttpResponse response) {
		}

		@Override
		protected void onCharReceived(final CharBuffer buf, final IOControl ioctrl) throws IOException {
			this.url.result += buf.toString();
		}

		@Override
		protected void releaseResources() {
		}

		@Override
		protected Boolean buildResult(final HttpContext context) {
			return Boolean.TRUE;
		}

	}
	
	/**
	 * 下载成功或失败
	 * @author luoyixin
	 *
	 */
	class MyFutureCallback implements FutureCallback {
		
		private Url url;
		
		public MyFutureCallback(Url url) {
			// TODO Auto-generated constructor stub
			this.url = url;
		}

		@Override
		public void cancelled() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void completed(Object arg0) {
			// 下载成功
			this.url.callback.onReceived(this.url.result, this.url.url);
		}

		@Override
		public void failed(Exception exception) {
			// 下载失败
			this.url.callback.onFailed(exception, this.url.url);
		}
		
	}

	// 存放带处理的url下载任务
	private BlockingQueue<Url> urlQueue = new LinkedBlockingQueue<>();
	// http client builder
	private HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();

	/**
	 * 向下载器添加url，和处理的回调
	 * @param url
	 * @param callback
	 */
	public void addUrl(String url, DownloadCallback callback) {
		this.urlQueue.add(new Url(url, callback));
	}
	
	/**
	 * 设置cookie
	 * @param cookies
	 */
	public void setCookie(Map<String, String> cookies) {
		CookieStore cookieStore = new BasicCookieStore();
		for (Map.Entry<String, String> each : cookies.entrySet()) {
			cookieStore.addCookie(new BasicClientCookie(each.getKey(), each.getValue()));
		}
		httpAsyncClientBuilder.setDefaultCookieStore(cookieStore);
	}
	
	/**
	 * 设置user agent
	 * @param userAgent
	 */
	public void setUserAgent(String userAgent) {
		httpAsyncClientBuilder.setUserAgent(userAgent);
	}
	
	/**
	 * 设置proxy
	 * @param proxy
	 */
	public void setProxy(String proxy) {
		HttpHost host = new HttpHost(proxy);
		httpAsyncClientBuilder.setProxy(host);
	}

	@SuppressWarnings("unchecked")
	public void begin() throws InterruptedException {
		CloseableHttpAsyncClient httpclient = httpAsyncClientBuilder.build();
		httpclient.start();
		new Thread(() -> {
			while (true) {
				try {
					Url url = this.urlQueue.take();
					httpclient.execute(HttpAsyncMethods.createGet(url.url), new MyResponseConsumer(url), new MyFutureCallback(url));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();

	}

}
