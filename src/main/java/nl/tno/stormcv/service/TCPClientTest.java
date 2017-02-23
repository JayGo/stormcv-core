package nl.tno.stormcv.service;

import edu.fudan.jliu.message.BaseMessage;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.constant.ZKConstant;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/15/17 - 8:04 AM
 * Description:
 */
public class TCPClientTest {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("127.0.0.1", 9000);
            String rtspUrl = GlobalConstants.PseudoRtspAddress;
            String rtmpAddress = ZKConstant.DefaultRTMPServer;
            String appName = "tcptest";
            BaseMessage streamIdMsg = new BaseMessage(7, rtspUrl,
                    rtmpAddress+appName, appName);
            OutputStream os =socket.getOutputStream();
            ObjectOutputStream objOs = new ObjectOutputStream(os);
            objOs.writeObject(streamIdMsg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
