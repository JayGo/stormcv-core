package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

import java.io.Serializable;

public class SourceInfo implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -4198085328451136021L;
    /**
     *
     */
    private int frameWidth;
    private int frameHeight;
    private String encodeQueueId;
    private String videoAddr;
    private CodecType type;

    public SourceInfo() {

    }

    public SourceInfo(int frameWidth, int frameHeight, String encodeQueueId, CodecType type) {
        super();
        this.frameWidth = frameWidth;
        this.frameHeight = frameHeight;
        this.encodeQueueId = encodeQueueId;
        this.type = type;
    }


    public String getVideoAddr() {
        return videoAddr;
    }

    public void setVideoAddr(String videoAddr) {
        this.videoAddr = videoAddr;
    }

    public CodecType getType() {
        return type;
    }

    public void setType(CodecType type) {
        this.type = type;
    }

    public int getFrameWidth() {
        return frameWidth;
    }

    public void setFrameWidth(int frameWidth) {
        this.frameWidth = frameWidth;
    }

    public int getFrameHeight() {
        return frameHeight;
    }

    public void setFrameHeight(int frameHeight) {
        this.frameHeight = frameHeight;
    }

    public String getEncodeQueueId() {
        return encodeQueueId;
    }

    public void setEncodeQueueId(String encodeQueueId) {
        this.encodeQueueId = encodeQueueId;
    }

    @Override
    public String toString() {
        return "SourceInfo [frameWidth=" + frameWidth + ", frameHeight=" + frameHeight + ", encodeQueueId="
                + encodeQueueId + ", videoAddr=" + videoAddr + ", type=" + type + "]";
    }


}
