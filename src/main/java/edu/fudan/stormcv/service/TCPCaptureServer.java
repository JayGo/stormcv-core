package edu.fudan.stormcv.service;

import edu.fudan.jliu.message.BaseMessage;
import edu.fudan.jliu.message.EffectMessage;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.constant.RequestCode;
import edu.fudan.stormcv.constant.ResultCode;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.service.db.DBManager;
import edu.fudan.stormcv.service.model.CameraInfo;
import edu.fudan.stormcv.service.model.EffectRtmpInfo;
import edu.fudan.stormcv.service.process.ProcessManager;
import edu.fudan.stormcv.service.process.WorkerInfo;
import edu.fudan.stormcv.spout.SpoutSignalClient;
import edu.fudan.stormcv.topology.TopologyH264;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private Map<String, TopologyH264> topologies;

    public TCPCaptureServer() {
        try {
            msgServerSocket = new ServerSocket(serverMsgPort);
            topologys = new ArrayList<TopologyH264>();

            topologies = new HashMap<>();
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
//            MessageReceiver receiver = new MessageReceiver(socket);
//            new Thread(receiver).start();
            MessageHandler handler = new MessageHandler(socket);
            new Thread(handler).start();
        }
    }

    private class MessageHandler implements Runnable {

        private Socket socket = null;

        public MessageHandler(Socket socket) {
            this.socket = socket;
            logger.info("Now thread#: " + Thread.currentThread().getId());
        }

        @Override
        public void run() {
            String request = receiveMessage();
            logger.info("receive message {}", request);
            JSONObject jRequest = new JSONObject(request);
            if (!jRequest.isNull("code")) {
                int code = jRequest.getInt("code");
                String result = null;
                String streamId = jRequest.getString("streamId");;
                switch (code) {
                    case RequestCode.START_RAW :
                        result = pushRawRtmpStream(streamId);
                        break;
                    case RequestCode.END_RAW :
                        String host = jRequest.getString("host");
                        long pid = jRequest.getLong("pid");
                        result = stopRawtmp(streamId, host, pid);
                        break;
                    case RequestCode.START_EFFECT :
                        logger.info("receive start effect request: {}", jRequest.toString());
                        String effectType = jRequest.getString("effectType");
                        Map<String, Object> effectParams = jRequest.getJSONObject("effectParams").toMap();
                        result = startEffectTopology(streamId, effectType, effectParams);
                        break;
                    case RequestCode.END_EFFECT :
                        logger.info("receive stop effect request: {}", jRequest.toString());
                        int id = jRequest.getInt("id");
                        result = stopEffectTopology(id);
                        break;

                    default:
                        logger.error("Unsupport request: {}!", request);
                        break;
                }

                if (result != null) {
                    sendMessage(result);
                } else {
                    logger.error("no result for the request");
                }
            }
            this.close();
        }

        String receiveMessage() {
//            StringBuilder sb = new StringBuilder();
//            try {
//                Reader reader = new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8);
//                char chars[] = new char[128];
//                int len;
//                while ((len = reader.read(chars)) != -1) {
//                    sb.append(new String(chars, 0, len));
//                }
//            } catch (IOException e)  {
//                e.printStackTrace();
//            }
            String result = null;
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                result = bufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("finished receive socket message, read:{}", result);
            return result;
//            logger.info("finished receive socket message, read:{}", sb.toString());
//            return sb.toString();
        }

        void sendMessage(String msg) {
            try {
//                Writer writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
//                writer.write(msg);
//                writer.flush();
                PrintWriter printWriter =new PrintWriter(socket.getOutputStream(),true);
                printWriter.println(msg);
                printWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("finished send socket message {}", msg);
        }

        void close() {
            try {
                this.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private String pushRawRtmpStream(String streamId) {
            JSONObject retJson = new JSONObject();
            retJson.put("code", RequestCode.RET_START_RAW);
            retJson.put("streamId", streamId);

            CameraInfo info = DBManager.getInstance().getCameraInfo(streamId);
            if (info.isValid()) {
                String address = info.getAddress();
                float frameRate = info.getFrameRate();
                String rtmpAddr = GlobalConstants.DefaultRTMPServer + streamId;
                String command = "ffmpeg -re -i " + address + " -qscale 0 -f flv -r " + frameRate + " -s " + info.getWidth() + "x" + info.getHeight() + " -an " + rtmpAddr;
                logger.info("command: {}", command);
                WorkerInfo worker = ProcessManager.getInstance().startProcess(command);
                retJson.put("host", worker.getHostIp());
                retJson.put("pid", worker.getPid());
                retJson.put("rtmpAddress", rtmpAddr);
                retJson.put("valid", true);
                retJson.put("status", ResultCode.RESULT_SUCCESS);
            } else {
                retJson.put("status", ResultCode.RESULT_FAILED);
            }
            return retJson.toString();
        }

        public String stopRawtmp(String streamId, String host, long pid) {
            boolean isKilled = ProcessManager.getInstance().killProcess(host, pid);
            JSONObject retJson = new JSONObject();
            retJson.put("code", RequestCode.RET_END_RAW);
            retJson.put("streamId", streamId);
            if (isKilled) {
                retJson.put("status", ResultCode.RESULT_SUCCESS);
            } else {
                retJson.put("status", ResultCode.RESULT_FAILED);
            }
            return retJson.toString();
        }

        public String startEffectTopology(String streamId, String effectType, Map<String, Object> effectParams) {
            JSONObject retJson = new JSONObject();
            retJson.put("code", RequestCode.RET_START_EFFECT);
            retJson.put("streamId", streamId);
            String topoName = streamId + "_" + effectType + "_" + effectParams.hashCode();
            String rtmpServerAddress = GlobalConstants.DefaultRTMPServer;

            CameraInfo info = DBManager.getInstance().getCameraInfo(streamId);
            if (info.isValid()) {
                String address = info.getAddress();
                TopologyH264 topologyH264 = new TopologyH264(topoName, rtmpServerAddress, address, effectType, effectParams, info.getFrameRate());
                topologys.add(topologyH264);

                topologies.put(topoName, topologyH264);

                try {
                    logger.info("submit topology {}, address = {}, rtmp = {}", topoName, address, rtmpServerAddress);
                    topologyH264.submitTopology();
                } catch (Exception e) {
                    e.printStackTrace();
                    retJson.put("status", ResultCode.RESULT_FAILED);
                    return retJson.toString();
                }

                retJson.put("effectType", effectType);
                retJson.put("effectParams", new JSONObject(effectParams));
                retJson.put("rtmpAddress", (rtmpServerAddress + topoName));
                retJson.put("topoName", topoName);
                retJson.put("valid", true);
                retJson.put("status", ResultCode.RESULT_SUCCESS);

            } else {
                retJson.put("status", ResultCode.RESULT_FAILED);
            }

            return retJson.toString();
        }

        public String stopEffectTopology(int id) {
            JSONObject retJson = new JSONObject();
            retJson.put("code", RequestCode.RET_END_EFFECT);

            EffectRtmpInfo info = DBManager.getInstance().getCameraEffectRtmpInfo(id);
            String topoName = info.getTopoName();
            retJson.put("topoName", topoName);
             TopologyH264 topology = topologies.get(topoName);
            if (topology != null) {
                String retStr = topology.killTopology();
                if (retStr.contains("Error")) {
                    retJson.put("status", ResultCode.RESULT_FAILED);
                } else {
                    retJson.put("status", ResultCode.RESULT_SUCCESS);
                }
            } else {
                retJson.put("status", ResultCode.RESULT_FAILED);
            }

            return retJson.toString();
        }
    }


/*    private class MessageReceiver implements Runnable {
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
    }*/


}
