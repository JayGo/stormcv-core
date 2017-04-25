package edu.fudan.stormcv.service.model;

import org.json.JSONObject;

public class CameraInfo {
	private String streamId;
	private String address;
	private boolean valid;
	private int width;
	private int height;
	private float frameRate;
	
	public CameraInfo() {
	}
	
	public CameraInfo(String streamId, String address,boolean valid) {
		this.streamId = streamId;
		this.address = address;
		this.valid = valid;
		this.width = 0;
		this.height = 0;
		this.frameRate = 0.0f;
	}
	
	public CameraInfo(String streamId, String address, int width, int height, float frameRate, boolean valid) {
		this.streamId = streamId;
		this.address = address;
		this.width = width;
		this.height = height;
		this.frameRate = frameRate;
		this.valid = valid;
	}
	
	public String getStreamId() {
		return streamId;
	}
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	public int getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public int getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public float getFrameRate() {
		return frameRate;
	}
	public void setFrameRate(float frameRate) {
		this.frameRate = frameRate;
	}
	
	@Override
	public String toString() {
		return "CameraInfo [streamId=" + streamId + ", address=" + address + ", valid=" + valid + ", width=" + width
				+ ", height=" + height + ", frameRate=" + frameRate + "]";
	}
	
	public String toJson() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("streamId", this.streamId);
		jsonObject.put("address", this.address);
		jsonObject.put("valid", this.valid);
		jsonObject.put("width", this.width);
		jsonObject.put("height", this.height);
		jsonObject.put("frameRate", this.frameRate);
		return jsonObject.toString();
	}
}
