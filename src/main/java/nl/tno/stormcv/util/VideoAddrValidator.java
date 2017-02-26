package nl.tno.stormcv.util;

import org.opencv.highgui.VideoCapture;

public class VideoAddrValidator {

    private static VideoCapture capture;

    public static boolean isVideoAddrValid(String videoAddr) {
        try {
            capture = new VideoCapture(videoAddr);
            capture.open(videoAddr);
            if (capture.isOpened() && capture.grab()) {
                return true;
            }
        } catch (Exception e) {

        }
        return false;
    }
}
