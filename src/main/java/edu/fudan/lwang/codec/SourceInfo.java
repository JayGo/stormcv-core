package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import scala.collection.generic.BitOperations.Int;

import java.io.Serializable;

public class SourceInfo implements Serializable {

    public CodecType getCodecType() {
		return codecType;
	}

	public void setCodecType(CodecType codecType) {
		this.codecType = codecType;
	}



	@Override
	public String toString() {
		return "SourceInfo [frameWidth=" + frameWidth + ", frameHeight=" + frameHeight + ", matType=" + matType
				+ ", yuvFrameWidth=" + yuvFrameWidth + ", yuvFrameHeight=" + yuvFrameHeight + ", yuvMatType="
				+ yuvMatType + ", sourceId=" + sourceId + ", videoAddr=" + videoAddr + ", codecType=" + codecType + "]";
	}



	/**
     *
     */
    private static final long serialVersionUID = -4198085328451136021L;
    /**
     *
     */
    private int frameWidth;
    private int frameHeight;
    private int matType;
    
    private int yuvFrameWidth;
    private int yuvFrameHeight;
    private int yuvMatType;
    
    private String sourceId;
    private String videoAddr;
    private CodecType codecType;

    public int getMatType() {
		return matType;
	}

	public void setMatType(int matType) {
		this.matType = matType;
	}

	public int getYuvMatType() {
		return yuvMatType;
	}

	public void setYuvMatType(int yuvMatType) {
		this.yuvMatType = yuvMatType;
	}

	public SourceInfo() {

    }

    public SourceInfo(int frameWidth, int frameHeight, int yuvFrameWidth, int yuvFrameHeigth, String sourceId, CodecType codecType, int matType, int yuvMatType) {
        super();
        this.frameWidth = frameWidth;
        this.frameHeight = frameHeight;
        this.yuvFrameWidth = yuvFrameWidth;
        this.yuvFrameHeight = yuvFrameHeigth;
        this.sourceId = sourceId;
        this.codecType = codecType;
        this.matType = matType;
        this.yuvMatType = yuvMatType;
    }


    public int getYuvFrameWidth() {
		return yuvFrameWidth;
	}

	public void setYuvFrameWidth(int yuvFrameWidth) {
		this.yuvFrameWidth = yuvFrameWidth;
	}

	public int getYuvFrameHeight() {
		return yuvFrameHeight;
	}

	public void setYuvFrameHeight(int yuvFrameHeight) {
		this.yuvFrameHeight = yuvFrameHeight;
	}

	public String getVideoAddr() {
        return videoAddr;
    }

    public void setVideoAddr(String videoAddr) {
        this.videoAddr = videoAddr;
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

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }




}
