package edu.fudan.stormcv.service;

import edu.fudan.jliu.message.BaseMessage;
import edu.fudan.jliu.message.EffectMessage;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.constant.RequestCode;
import edu.fudan.stormcv.constant.ResultCode;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.spout.SpoutSignalClient;
import edu.fudan.stormcv.topology.TCPCaptureTopology;
import edu.fudan.stormcv.topology.TopologyH264;

import org.apache.storm.shade.com.codahale.metrics.MetricRegistryListener.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lwang
 */
public class TCPCaptureServer {

//	public static void main(String [] args) {
//		TCPServer ts = new TCPServer();
//		ts.startListeningMsg();
//	}

    private static Logger logger = LoggerFactory.getLogger(TCPCaptureServer.class);

    private final int serverMsgPort = 8999;
    private ServerSocket msgServerSocket;

    private static TCPCaptureServer mTCPCaptureServer;

    private List<TopologyH264> topologys;

    public TCPCaptureServer() {
        try {
            msgServerSocket = new ServerSocket(serverMsgPort);
            topologys = new ArrayList<TopologyH264>();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static synchronized TCPCaptureServer getInstance() {
        if (mTCPCaptureServer == null) {
            mTCPCaptureServer = new TCPCaptureServer();
        }
        return mTCPCaptureServer;
    }

    public void startListeningMsg() {
        while (true) {
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
            logger.info("Receive message: " + bMsg);
            if (null == bMsg) {
                logger.error("receiveMessage returns null!");
                return;
            }
            BaseMessage respond = handleMessage(bMsg);
            // BaseMessage respond = new BaseMessage(ResultCode.RESULT_OK);
            logger.info("strom respond: " + respond);
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
//                oos.flush();
//                oos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        private BaseMessage handleMessage(BaseMessage msg) {
            BaseMessage result = new BaseMessage(ResultCode.NO_SERVER_AVAILABLE);
            switch (msg.getCode()) {
                case RequestCode.START_STORM: {
                    result = pushRawStream(msg);
                    break;
                }

                case RequestCode.START_EFFECT_STORM: {
                    result = startTopology((EffectMessage) msg);
                    break;
                }
                
                case RequestCode.PIC_PROCESS: {
                	result = processPicture((EffectMessage) msg);
                	break;
                }

                case RequestCode.END_STORM:
                case RequestCode.END_EFFECT_STORM: {
                    result = stopCaptureTopology(msg);
                    break;
                }
                

                default: {
                    logger.error("Unknow message!");
                }
            }

            return result;
        }
        
        private BaseMessage processPicture(EffectMessage fmsg) {
        	BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
        	
        	int processType = BOLT_OPERTION_TYPE.getOpTypeByString(fmsg.getEffectType()).getCode();
        	String streamId = fmsg.getStreamId();
        	
        	String srcPath = !fmsg.getAddr().startsWith("file://") ? "file://"+fmsg.getAddr() : fmsg.getAddr();
        	String dstPath = !fmsg.getRtmpAddr().startsWith("file://") ? "file://"+fmsg.getRtmpAddr() : fmsg.getRtmpAddr();
        	
        	String zkHost = GlobalConstants.ZKHostLocal;
        	ImageRequest request = new ImageRequest(streamId, srcPath, dstPath, processType);
            SpoutSignalClient sc = new SpoutSignalClient(zkHost, GlobalConstants.ImageRequestZKRoot);
            sc.start();
            
            try {
            	sc.sendImageRequest(request);
            } finally {
            	sc.close();
            }
            
            result.setCode(ResultCode.RESULT_OK);
        	return result;
        }
        
        
        private BaseMessage pushRawStream(BaseMessage msg) {
        	BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
        	String srcRtspAddr = msg.getAddr();
        	if(!srcRtspAddr.startsWith("rtsp://")) {
        		logger.error("illegal rtsp name!");
        		return result;
        	}
        	String dstRtmpAddr = GlobalConstants.DefaultRTMPServer+msg.getStreamId();
        	String streamPushCommand = "ffmpeg -i "+ srcRtspAddr +  " -qscale 0 -f flv -r 25 -an " + dstRtmpAddr;
        	try {
				Process p = Runtime.getRuntime().exec(streamPushCommand);
				System.out.println("ffmpeg push process ID: "+p.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("exception in exec ffmpeg push command!");
				return result;
			}
        	result.setAddr(msg.getAddr());
        	result.setStreamId(msg.getStreamId());
        	result.setCode(ResultCode.RESULT_OK);
        	result.setRtmpAddr(GlobalConstants.DefaultRTMPServer);
        	return result;
        }

        private BaseMessage startTopology(BaseMessage msg) {
            BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
            msg.setRtmpAddr(GlobalConstants.DefaultRTMPServer);
            TopologyH264 mTopologyH264 = new TopologyH264(msg);

            topologys.add(mTopologyH264);
            try {
            	mTopologyH264.submitTopology();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return result;
            }
            result.setRtmpAddr(GlobalConstants.DefaultRTMPServer);
            result.setCode(ResultCode.RESULT_OK);
            return result;
        }

        private BaseMessage stopCaptureTopology(BaseMessage msg) {
            BaseMessage result = new BaseMessage(ResultCode.UNKNOWN_ERROR);
            String streamId = msg.getStreamId();
            String killResultStr = "";
            for (int i = 0; i < topologys.size(); i++) {
            	TopologyH264 mTopologyH264 = topologys.get(i);
                if (mTopologyH264.getStreamId().equals(streamId)) {
                    killResultStr += mTopologyH264.killTopology();
                    if (!killResultStr.contains("Error")) {
                        topologys.remove(mTopologyH264);
                    } else {
                        return result;
                    }
                }
            }

            result.setCode(ResultCode.RESULT_OK);
            return result;
        }
    }


}
