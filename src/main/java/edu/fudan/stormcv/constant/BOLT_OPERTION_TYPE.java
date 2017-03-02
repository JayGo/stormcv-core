package edu.fudan.stormcv.constant;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/1/17 - 10:28 AM
 * Description:
 */
public enum BOLT_OPERTION_TYPE {
    GRAY("gray"), FACEDETECT("faceDetect"), COLORHISTOGRAM("colorHistogram"),
    SCALE("scale"), RTMPSTREAMER("rtmpStreamer"), MJPEGSTREAMER("mjpegStreamer"), UNSUPPORT("unsupport");

    private String operationName = "";

    private BOLT_OPERTION_TYPE(String operationName) {
        this.operationName = operationName;
    }

    @Override
    public String toString() {
        return this.operationName;
    }
}
