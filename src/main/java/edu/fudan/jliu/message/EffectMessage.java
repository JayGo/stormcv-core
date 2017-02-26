package edu.fudan.jliu.message;

import java.util.HashMap;

public class EffectMessage extends BaseMessage {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private String effectType;
    private HashMap<String, Double> parameters;

    public EffectMessage(int code, String addr, String rtmpAddr, String streamId, String effectType, HashMap<String, Double> parameters) {
        super(code, addr, rtmpAddr, streamId);
        this.effectType = effectType;
        this.parameters = parameters;
    }

    public EffectMessage(int code, String addr, String rtmpAddr, String effectType, HashMap<String, Double> parameters) {
        super(code, addr, rtmpAddr, null);
        this.effectType = effectType;
        this.parameters = parameters;
    }

    public EffectMessage(int code, String addr, String effectType, HashMap<String, Double> parameters) {
        super(code, addr);
        this.effectType = effectType;
        this.parameters = parameters;
    }

    public String getEffectType() {
        return effectType;
    }

    public void setEffectType(String effectType) {
        this.effectType = effectType;
    }

    public HashMap<String, Double> getParameters() {
        return parameters;
    }

    public void setParameters(HashMap<String, Double> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        StringBuilder sBuilder = new StringBuilder(super.toString());
        sBuilder.append(" effctType: " + effectType);
        sBuilder.append(" parameters' size: " + parameters.size());
        return sBuilder.toString();
    }
}
