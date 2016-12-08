package edu.fudan.jliu.message;

import java.io.Serializable;

public class BaseMessage implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int code;
	private String addr;
	private String rtmpAddr;
	private String streamId;
	
	public BaseMessage(int code, String addr, String rtmpAddr, String streamId) {
		this.code = code;
		this.addr = addr;
		this.rtmpAddr = rtmpAddr;
		this.streamId = streamId;
	}
	
	public BaseMessage(int code) {
		this(code, null);
	}
	
	public BaseMessage(int code, String addr, String rtmpAddr) {
		this(code, addr, rtmpAddr,null);
	}
	
	public BaseMessage(int code, String addr) {
		this(code, addr, null,null);
	}
	
	public BaseMessage(String streamId, int code, String addr) {
		this(code, addr, null,streamId);
	}
	
	public int getCode() {
		return code;
	}
	
	public void setCode(int code) {
		this.code = code;
	}
	
	public String getAddr() {
		return this.addr;
	}
	
	public void setAddr(String addr) {
		this.addr = addr;
	}
	
	public void setRtmpAddr(String rtmpAddr) {
		this.rtmpAddr = rtmpAddr;
	}
	
	public String getRtmpAddr() {
		return this.rtmpAddr;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}
	
	public String getStreamId() {
		return this.streamId;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(" code: "+code);
		sBuilder.append(" addr: "+addr);
		sBuilder.append(" rtmpAddr: "+rtmpAddr);
		sBuilder.append(" streamId: "+streamId);
		return sBuilder.toString();
	}
}

