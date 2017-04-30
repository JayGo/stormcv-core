package edu.fudan.stormcv.constant;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/1/17 - 10:28 AM
 * Description:
 */
public enum BoltOperationType {
    /*Basic Image Operation*/
    GRAY(1, "gray"),
    FACEDETECT(2, "faceDetect"),
    COLORHISTOGRAM(3, "colorHistogram"),
    SCALE(4, "scale"),

    /*Basic Video Write Mathod*/
    RTMPSTREAMER(101, "rtmpStreamer"),
    MJPEGSTREAMER(102, "mjpegStreamer"),
    VIDEOFILEGEN(111, "videoFileGen"),
	UNSUPPORT(999, "unsupport"),

    /*for custom operation*/
    CUSTOM(1111, "custom");

    private String operationName = "";
    private int code;

    BoltOperationType(int code, String operationName) {
        this.code = code;
        this.operationName = operationName;
    }

    public static BoltOperationType getOpType(int code) {
        for (BoltOperationType type : BoltOperationType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return null;
    }
    
    public static BoltOperationType getOpTypeByString(String effectType) {
        for (BoltOperationType type : BoltOperationType.values()) {
            if (type.toString().equals(effectType)) {
                return type;
            }
        }
    	return UNSUPPORT;
    }

    public int getCode() {
        return this.code;
    }

    @Override
    public String toString() {
        return this.operationName;
    }
}
