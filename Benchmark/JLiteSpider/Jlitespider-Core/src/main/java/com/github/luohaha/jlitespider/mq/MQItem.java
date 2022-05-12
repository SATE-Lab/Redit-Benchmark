package com.github.luohaha.jlitespider.mq;

public class MQItem {
	private String key;
	private Object value;
	
	public MQItem(String key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
}
