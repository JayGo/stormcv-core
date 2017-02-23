package nl.tno.stormcv.service;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import edu.fudan.jliu.message.BaseMessage;
import edu.fudan.jliu.message.EffectMessage;
import nl.tno.stormcv.constant.RequestCode;
import nl.tno.stormcv.constant.ResultCode;
import nl.tno.stormcv.topology.TCPReaderTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author lwang
 *
 */
public class TCPServer {
	
//	public static void main(String [] args) {
//		TCPServer ts = new TCPServer();
//		ts.startListeningMsg();
//	}
	
	private static Logger logger = LoggerFactory.getLogger(TCPServer.class);

	private final int serverMsgPort = 9000;
	private ServerSocket msgServerSocket;

	private static TCPServer mTCPServer;

	private List<TCPReaderTopology> topologys;

	public TCPServer() {
		try {
			msgServerSocket = new ServerSocket(serverMsgPort);
			topologys = new ArrayList<TCPReaderTopology>();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static synchronized TCPServer getInstance() {
		if (mTCPServer == null) {
			mTCPServer = new TCPServer();
		}
		return mTCPServer;
	}

	public void startListeningMsg() {
		while(true) {
			logger.info("Listening message request...");
			Socket socket = null;
			try {
				socket = msgServerSocket.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Accept from: " + socket.getInetAddress());
			MessageReceiver receiver = new MessageReceiver(socket);
			new Thread(receiver).start();
		}
	}
	
	
	private class MessageReceiver implements Runnable {
		private Socket socket = null;
		
		public MessageReceiver(Socket socket) {
			this.socket = socket;
			logger.info("Now thread#: " + Thread.currentThread().getId());
		}
		
		public void run() {
			// TODO Auto-generated method stub
			BaseMessage bMsg = receiveMessage();
			logger.info("Receive message: "+bMsg);
			if (null == bMsg) {
				logger.error("receiveMessage returns null!");
				return;
			}
			BaseMessage respond = handleMessage(bMsg);
			// BaseMessage respond = new BaseMessage(ResultCode.RESULT_OK);
			logger.info("strom respond: "+respond);
			answerMessage(respond);
//			try {
//				socket.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		
		private BaseMessage receiveMessage() {
			ObjectInputStream ois;
			BaseMessage msg = null;
			try {
				ois = new ObjectInputStream(socket.getInputStream());
				msg = (BaseMessage) ois.readObject();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return msg;
		}
		
		private void answerMessage(BaseMessage msg) {
			ObjectOutputStream oos;
			try {
				oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(msg);
				oos.flush();
				oos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private BaseMessage handleMessage(BaseMessage msg) {
			BaseMessage result = new BaseMessage(ResultCode.NO_SERVER_AVAILABLE);
			switch (msg.getCode()) {
				case RequestCode.START_STORM: {
					result = startReaderTopology(msg);
					break;
				}
	
				case RequestCode.START_EFFECT_STORM: {
					result = startReaderTopology((EffectMessage)msg);
					break;
				}
	
				case RequestCode.END_STORM:
				case RequestCode.END_EFFECT_STORM: {
					result = stopReaderTopology(msg);
					break;
				}
	
				default: {
					logger.error("Unknow message!");
				}
			}

			return result;
		}
	}
	
	private BaseMessage startReaderTopology(BaseMessage msg) {
		BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
		boolean isEffectMessage = msg instanceof EffectMessage;
		String videoAddr = msg.getAddr();
		String streamId = msg.getStreamId();
		String rtmpAddr = msg.getRtmpAddr();
		
		String serverIp = "";
		int port = -1;
		
		if (videoAddr.contains(":")) {
			try {
				serverIp = videoAddr.split(":")[0];
				port = Integer.parseInt(videoAddr.split(":")[1]);
				logger.info("serverIp: "+serverIp+", port: "+port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return result;
			}
		}
		
		TCPReaderTopology mTCPReaderTopology = null;
		
		if(!isEffectMessage) {
			mTCPReaderTopology = new TCPReaderTopology(streamId, rtmpAddr, serverIp, port);
		} else {
			String effect = ((EffectMessage) msg).getEffectType();
			mTCPReaderTopology = new TCPReaderTopology(streamId, rtmpAddr, effect, serverIp, port);
		}
		
		topologys.add(mTCPReaderTopology);
		try {
			mTCPReaderTopology.submitTopologyToCluster();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return result;
		}
		result.setCode(ResultCode.RESULT_OK);
		return result;
	}
	
	private BaseMessage stopReaderTopology(BaseMessage msg) {
		BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
		String streamId = msg.getStreamId();
		String killResultStr = "";
		for (int i = 0; i < topologys.size(); i++) {
			TCPReaderTopology mTCPReaderTopology = topologys.get(i);
			if (mTCPReaderTopology.getStreamId().equals(streamId)) {
				killResultStr += mTCPReaderTopology.killTopology();
				if (!killResultStr.contains("Error")) {
					topologys.remove(mTCPReaderTopology);
				}
				else {
					return result;
				}
			}
		}
		
		result.setCode(ResultCode.RESULT_OK);
		return result;
	}
}
