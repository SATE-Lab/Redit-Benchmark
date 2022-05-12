package com.github.luohaha.jlitespider.setting;
/**
 * 与配置文件中读取出来的信息，一一对应
 * @author luoyixin
 *
 */
public class SettingObject {
	private int workerid;
	private MqObject[] mq;
	private String[] sendto;
	private String[] recvfrom;
	public int getWorkerid() {
		return workerid;
	}
	public void setWorkerid(int workerid) {
		this.workerid = workerid;
	}
	public MqObject[] getMq() {
		return mq;
	}
	public void setMq(MqObject[] mq) {
		this.mq = mq;
	}
	public String[] getSendto() {
		return sendto;
	}
	public void setSendto(String[] sendto) {
		this.sendto = sendto;
	}
	public String[] getRecvfrom() {
		return recvfrom;
	}
	public void setRecvfrom(String[] recvfrom) {
		this.recvfrom = recvfrom;
	}
	
}
