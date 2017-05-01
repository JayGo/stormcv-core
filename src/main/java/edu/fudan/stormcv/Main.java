package edu.fudan.stormcv;


import edu.fudan.stormcv.service.TCPCaptureServer;
import edu.fudan.stormcv.service.monitor.ProcessMonitor;
import edu.fudan.stormcv.util.LibLoader;

//import nl.tno.stormcv.service.TCPServer;


public class Main {

    public static void main(String args[]) {
        LibLoader.loadOpenCVLib();
        LibLoader.loadHgCodecLib();

        Thread monitorThread = new Thread(new ProcessMonitor());
        monitorThread.setName("process-monitor");
        monitorThread.start();
        TCPCaptureServer mTCPCaptureServer = TCPCaptureServer.getInstance();
        mTCPCaptureServer.startListeningMsg();

//		TCPServer mTCPServer = TCPServer.getInstance();
//		mTCPServer.startListeningMsg();

    }
}
