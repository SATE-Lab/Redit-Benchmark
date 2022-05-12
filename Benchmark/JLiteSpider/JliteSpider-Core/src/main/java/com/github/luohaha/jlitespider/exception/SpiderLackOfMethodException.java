package com.github.luohaha.jlitespider.exception;

public class SpiderLackOfMethodException extends Exception {
	public SpiderLackOfMethodException () {
		super("Spider lack of method : [downloader, processor, saver, freeman]!");
	}
}
