package com.github.luohaha.jlitespider.core;

import java.io.IOException;

public interface MessageQueue {
	public void sendUrl(Object url) throws IOException;
	public void sendPage(Object page) throws IOException;
	public void sendResult(Object result) throws IOException;
	public void send(String key, Object msg) throws IOException;
}
