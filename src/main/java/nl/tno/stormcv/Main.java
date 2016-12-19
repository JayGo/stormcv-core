package nl.tno.stormcv;

import nl.tno.stormcv.service.TCPCaptureServer;

public class Main {

	public static void main(String args[]) {
		System.load("/usr/local/opencv/share/OpenCV/java/libopencv_java2413.so");
		System.load("/usr/local/LwangCodec/lib/libHgCodec.so");
		
		TCPCaptureServer mTCPCaptureServer = TCPCaptureServer.getInstance();
		mTCPCaptureServer.startListeningMsg();
	}
}
