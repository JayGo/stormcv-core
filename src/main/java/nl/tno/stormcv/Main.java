package nl.tno.stormcv;

import nl.tno.stormcv.service.TCPServer;

public class Main {

	public static void main(String args[]) {
		
		TCPServer mTCPServer = TCPServer.getInstance();
		mTCPServer.startListeningMsg();
	}
}
