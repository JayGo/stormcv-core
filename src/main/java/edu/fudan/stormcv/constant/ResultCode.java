package edu.fudan.stormcv.constant;

public class ResultCode {
    public static final int RESULT_OK = 0;
    public static final int CAM_ADDR_IS_EXSITED = -1;
    public static final int NO_SERVER_AVAILABLE = -2;
    public static final int CAPTURE_FAILED = -3;
    public static final int START_EFFECT_STREAMING_FAILED = -4;
    public static final int UNKNOWN_ERROR = -5;


    public static final int RESULT_SUCCESS = 0;
    public static final int RESULT_FAILED = 1;
    public static final int RESULT_ERROR_DATABASE = 2;
    public static final int RESULT_ERROR_UNSUPPORT_EFFECT = 3;
    public static final int RESULT_ERROR_STREAMID = 4;
    public static final int RESULT_ERROR_CAMERA_INVALID = 5;
    public static final int RESULT_UNKNOWN_ERROR = 999;
}
