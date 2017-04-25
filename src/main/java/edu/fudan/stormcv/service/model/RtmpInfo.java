package edu.fudan.stormcv.service.model;

public class RtmpInfo {
	protected String streamId;
	protected String rtmpAddress;
	protected boolean valid;
	
	public RtmpInfo() {

	}
		
	public RtmpInfo(String streamId, String rtmpAddress, boolean valid) {
		super();
		this.streamId = streamId;
		this.rtmpAddress = rtmpAddress;
		this.valid = valid;
	}
	public String getStreamId() {
		return streamId;
	}
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}
	public String getRtmpAddress() {
		return rtmpAddress;
	}
	public void setRtmpAddress(String rtmpAddress) {
		this.rtmpAddress = rtmpAddress;
	}
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	@Override
	public String toString() {
		return "RtmpInfo [streamId=" + streamId + ", rtmpAddress=" + rtmpAddress + ", valid="
				+ valid + "]";
	}
}
