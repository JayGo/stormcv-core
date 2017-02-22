package nl.tno.stormcv;


import nl.tno.stormcv.service.TCPCaptureServer;
import nl.tno.stormcv.service.TCPServer;


public class Main {

	public static void main(String args[]) {

		System.load("/usr/local/opencv/share/OpenCV/java/libopencv_java2413.so");
		System.load("/usr/local/LwangCodec/lib/libHgCodec.so");
		
		TCPCaptureServer mTCPCaptureServer = TCPCaptureServer.getInstance();
		mTCPCaptureServer.startListeningMsg();

		
		TCPServer mTCPServer = TCPServer.getInstance();
		mTCPServer.startListeningMsg();

	}
}
