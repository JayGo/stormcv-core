package edu.fudan.stormcv.constant;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/1/17 - 10:28 AM
 * Description:
 */
public enum BOLT_OPERTION_TYPE {
    /*Basic Image Operation*/
    GRAY(1, "gray"),
    FACEDETECT(2, "faceDetect"),
    COLORHISTOGRAM(3, "colorHistogram"),
    SCALE(4, "scale"),

    /*Basic Video Write Mathod*/
    RTMPSTREAMER(101, "rtmpStreamer"),
    MJPEGSTREAMER(102, "mjpegStreamer"),
    VIDEOFILEGEN(111, "videoFileGen");

    private String operationName = "";
    private int code;

    private BOLT_OPERTION_TYPE(int code, String operationName) {
        this.code = code;
        this.operationName = operationName;
    }

    public static BOLT_OPERTION_TYPE getOpType(int code) {
        for (BOLT_OPERTION_TYPE type : BOLT_OPERTION_TYPE.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return null;
    }

    public int getCode() {
        return this.code;
    }

    @Override
    public String toString() {
        return this.operationName;
    }
}
