package edu.fudan.stormcv.service.model;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class EffectRtmpInfo extends RtmpInfo {
	private int id = -1;
	private String effectType;
	private Map<String, Object> effectParams;
	private String topoName;
	
	public EffectRtmpInfo() {
		this.effectParams = new HashMap<String, Object>();
	}
	
	public EffectRtmpInfo(String streamId, String rtmpAddress, boolean valid, String effectType,
			Map<String, String> effectParams, String topoName) {
		super(streamId, rtmpAddress, valid);
		this.effectType = effectType;
		this.topoName = topoName;
		this.effectParams = new HashMap<>();
		if (effectParams != null) {
			this.effectParams.putAll(effectParams);
		}
	}

	public EffectRtmpInfo(int id, String streamId, String rtmpAddress, boolean valid, String effectType,
			Map<String, Object> effectParams, String topoName) {
		super(streamId, rtmpAddress, valid);
		this.id = id;
		this.effectType = effectType;
		this.topoName = topoName;
		this.effectParams = new HashMap<>();
		if (effectParams != null) {
			this.effectParams.putAll(effectParams);
		}
	}
	
	public EffectRtmpInfo(String jsonStr) {
		JSONObject jsonObject = new JSONObject(jsonStr);
		streamId = jsonObject.getString("streamId");
		effectType = jsonObject.getString("effectType");
		valid = jsonObject.getBoolean("valid");
		rtmpAddress = jsonObject.getString("rtmpAddress");
		this.topoName = jsonObject.getString("topoName");
		this.effectParams = new HashMap<>();
		JSONObject params = jsonObject.getJSONObject("effectParams");
		Map<String, Object> paramsMap = params.toMap();
		for (String paraKey : paramsMap.keySet()) {
			this.effectParams.put(paraKey, paramsMap.get(paraKey));
		}
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getEffectType() {
		return effectType;
	}

	public void setEffectType(String effectType) {
		this.effectType = effectType;
	}

	public Map<String, Object> getEffectParams() {
		return effectParams;
	}

	public void setEffectParams(Map<String, Object> effectParams) {
		if (effectParams != null) {
			this.effectParams.putAll(effectParams);
		}
	}

	public String getTopoName() {
		return topoName;
	}

	public void setTopoName(String topoName) {
		this.topoName = topoName;
	}

	@Override
	public String toString() {
		return "EffectRtmpInfo [id=" + id + ", streamId=" + getStreamId() + ", rtmpAddress="
				+ getRtmpAddress() + ", valid=" + isValid() + ", effectType=" + effectType + ", effectParams="
				+ (new JSONObject(effectParams).toString()) + ", topoName=" + topoName + "]";
	}

	
}
