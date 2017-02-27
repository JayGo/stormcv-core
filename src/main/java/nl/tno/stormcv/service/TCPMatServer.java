package nl.tno.stormcv.service;

import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.topology.MatReaderTopology;
import nl.tno.stormcv.util.VideoAddrValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;
import java.util.Map.Entry;

//import nl.tno.stormcv.constant.ZKConstant;
//import nl.tno.stormcv.topology.MatReaderTopology;

public class TCPMatServer {
    private static final Logger logger = LoggerFactory.getLogger(TCPMatServer.class);
    private final int serverMsgPort = 8967;
    private ServerSocket msgServerSocket;

    private static TCPMatServer mTCPServer;

    private List<MatReaderTopology> topologys;

    private String[] rtmpServers = {GlobalConstants.DefaultRTMPServer};
    private Map<String, Integer> rtmpServerMap;

    // private List<String> msgQueue = new ArrayList<String>();

    public TCPMatServer() {
        try {
            msgServerSocket = new ServerSocket(serverMsgPort);
            topologys = new ArrayList<MatReaderTopology>();

            rtmpServerMap = new HashMap<String, Integer>();
            for (int i = 0; i < rtmpServers.length; i++) {
                rtmpServerMap.put(rtmpServers[i], 0);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static synchronized TCPMatServer getInstance() {
        if (mTCPServer == null) {
            mTCPServer = new TCPMatServer();
        }
        return mTCPServer;
    }

    public void startListeningMsg() {

        while (true) {
            try {
                char[] receivedData = new char[1024];
                Socket clientSocket = msgServerSocket.accept();
                SocketAddress clientAddress = clientSocket
                        .getRemoteSocketAddress();

                BufferedReader in = new BufferedReader(new InputStreamReader(
                        clientSocket.getInputStream()));
                DataOutputStream out = new DataOutputStream(
                        clientSocket.getOutputStream());
                // receive until client close connection,indicate by -l return
                logger.info("Handling client at " + clientAddress);

                int numRead = in.read(receivedData);
                String receivedString = new String(receivedData, 0, numRead);
                logger.info("Received: " + receivedString);

                String msgCallBack = parseMsg(receivedString);
                logger.info("Answer : " + msgCallBack);
                byte[] data = msgCallBack.getBytes("utf-8");
                out.write(data);
                in.close();
                out.close();
                clientSocket.close();

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

    private String parseMsg(String msg) {
        if (msg.contains(",")) {
            String msgs[] = msg.split(",");
            if (msgs[0] != null && msgs[0].equals("start")) {
                String videoAddr = msgs[1];
                String streamId = getStreamIdByVideoAddr(videoAddr) + "_origin";
                String rtmpAddr = getSuitableRTMPServer();

                try {

                    MatReaderTopology mMatReaderTopology = new MatReaderTopology(
                            streamId, rtmpAddr, videoAddr);
                    topologys.add(mMatReaderTopology);
                    mMatReaderTopology.submitTopologyToCluster();

                    return "Succeed," + rtmpAddr + streamId;
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return "Error,failed to submit topology";

            } else if (msgs[0] != null && msgs[0].equals("end")) {
                String videoAddr = msgs[1];
                String streamId = getStreamIdByVideoAddr(videoAddr);

                String result = "";
                for (int i = 0; i < topologys.size(); i++) {
                    MatReaderTopology mMatReaderTopology = topologys.get(i);
                    if (mMatReaderTopology.getStreamId().startsWith(streamId)) {
                        result += mMatReaderTopology.killTopology();
                        if (!result.contains("Error")) {
                            topologys.remove(mMatReaderTopology);
                        }
                    }
                }
                return result.contains("Error") ? "Error" : "Succeed";
            } else if (msgs[0] != null && msgs[0].equals("startEffect")) {
                String videoAddr = msgs[1];
                String effect = msgs[2];

                String streamId = getStreamIdByVideoAddr(videoAddr) + "_" + effect;
                String rtmpAddr = getSuitableRTMPServer();

                try {
                    MatReaderTopology mMatReaderTopology = new MatReaderTopology(
                            streamId, rtmpAddr, videoAddr, effect);

                    topologys.add(mMatReaderTopology);
                    mMatReaderTopology.submitTopologyToCluster();
                    return "Succeed," + rtmpAddr + streamId;
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return "Error,failed to submit topology";

            } else if (msgs[0] != null && msgs[0].equals("valid")) {

                String videoAddr = msgs[1];
                return VideoAddrValidator.isVideoAddrValid(videoAddr) ? "true" : "false";
            }
        }
        return "Bad Message Received!";
    }

    private String getSuitableRTMPServer() {
        String rtmpAddr = rtmpServers[0];
        Iterator<Entry<String, Integer>> itRTMP = rtmpServerMap.entrySet()
                .iterator();
        int minimalNum = 100;
        while (itRTMP.hasNext()) {
            Map.Entry<String, Integer> entryRTMP = itRTMP.next();
            if (minimalNum > entryRTMP.getValue()) {
                minimalNum = entryRTMP.getValue();
            }
        }

        itRTMP = rtmpServerMap.entrySet().iterator();
        while (itRTMP.hasNext()) {
            Map.Entry<String, Integer> entryRTMP = itRTMP.next();
            if (minimalNum == entryRTMP.getValue()) {
                rtmpAddr = entryRTMP.getKey();
                rtmpServerMap.put(rtmpAddr, entryRTMP.getValue() + 1);
                break;
            }
        }
        return rtmpAddr;
    }

    private String getStreamIdByVideoAddr(String videoAddr) {
        return videoAddr.hashCode() < 0 ? ("" + videoAddr.hashCode()).replace(
                "-", "a") : videoAddr.hashCode() + "";

    }
}
