package edu.fudan.stormcv.constant;

public class GlobalConstants {
    public static final String RTSPAddress176 = "rtsp://admin:123456qaz@10.134.141.176:554/h264/ch1/main/av_stream";
    public static final String RTSPAddress177 = "rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream";
    public static final String RTSPAddress178 = "rtsp://admin:123456qaz@10.134.141.178:554/h264/ch1/main/av_stream";
    public static final String PseudoRtspAddress = "rtsp://10.134.142.114/planet.mkv";

    public static final String QueueTypeMat = "Mat";
    public static final String QueueTypeBufferedImage = "BufferedImage";

    public static final int MatToBIWorkerPerStream = 4;

    public static int SpoutQueueSize = 100;
    public static String DefaultRTMPServer = "rtmp://10.134.142.114:1935/live1/";

    public static int JPEGCodecQuality = 80;
}
