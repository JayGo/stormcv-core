package nl.tno.stormcv;


import nl.tno.stormcv.service.TCPCaptureServer;
import nl.tno.stormcv.util.LibLoader;

//import nl.tno.stormcv.service.TCPServer;


public class Main {

    public static void main(String args[]) {
        LibLoader.loadOpenCVLib();
        LibLoader.loadHgCodecLib();

        TCPCaptureServer mTCPCaptureServer = TCPCaptureServer.getInstance();
        mTCPCaptureServer.startListeningMsg();

//		TCPServer mTCPServer = TCPServer.getInstance();
//		mTCPServer.startListeningMsg();

    }
}
