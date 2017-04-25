package edu.fudan.stormcv.service.model;

import org.json.JSONObject;

public class RawRtmpInfo extends RtmpInfo {
	private String host;
	private long pid;
	
	public RawRtmpInfo() {
	}

	public RawRtmpInfo(String streamId, String rtmpAddress, boolean valid, String host, long pid) {
		super(streamId, rtmpAddress, valid);
		this.host = host;
		this.pid = pid;
	}
	
	public RawRtmpInfo(String jsonStr) {
		JSONObject jsonObject = new JSONObject(jsonStr);
		streamId = jsonObject.getString("streamId");
		valid = jsonObject.getBoolean("valid");
		rtmpAddress = jsonObject.getString("rtmpAddress");
		host = jsonObject.getString("host");
		pid = jsonObject.getLong("pid");
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getPid() {
		return pid;
	}

	public void setPid(long pid) {
		this.pid = pid;
	}

	@Override
	public String toString() {
		return "RawRtmpInfo [streamId=" + getStreamId() + ", rtmpAddress="
				+ getRtmpAddress() + ", valid=" + isValid() + ", host=" + host + ", pid=" + pid + "]";
	}
}
