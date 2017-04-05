package edu.fudan.stormcv.constant;

public class GlobalConstants {
    public static final String RTSPAddress176 = "rtsp://admin:123456qaz@10.134.141.176:554/h264/ch1/main/av_stream";
    public static final String RTSPAddress177 = "rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream";
    public static final String RTSPAddress178 = "rtsp://admin:123456qaz@10.134.141.178:554/h264/ch1/main/av_stream";

    public static final String PseudoRtspAddress = "rtsp://10.134.142.114/planet-sub.mkv";
    public static final String Pseudo720pRtspAddress = "rtsp://10.134.142.114/planet720-sub.mkv";
    //for face detect
    public static final String PseudoFaceRtspAddress = "rtsp://10.134.142.114/bigbang480.mkv";
    public static final String Pseudo720pFaceRtspAddress = "rtsp://10.134.142.114/bigbang720.mkv";

    public static final String QueueTypeMat = "Mat";
    public static final String QueueTypeBufferedImage = "BufferedImage";

    public static final int MatToBIWorkerPerStream = 4;

    public static int SpoutQueueSize = 100;
    public static String DefaultRTMPServer = "rtmp://10.134.142.141/live/";
    public static String OldRTMPServer = "rtmp://10.134.142.114:1935/live1/";

    public static int JPEGCodecQuality = 80;

    public static final String HaarCacascadeXMLFileName = "lbpcascade_frontalface.xml";

    public static String ZKHostLocal = "localhost:2001";
    public static String ZKHostCluster141 = "10.134.142.141:2181";

    public static String TestImageInputDir = "file:///home/nfs/images/720p/";
    public static String TestImageOuputDir = "file:///home/nfs/images/720p/output/";

    public static String ImageRequestZKRoot = "imageRequest";
}
