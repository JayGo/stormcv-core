package nl.tno.stormcv.service;

import edu.fudan.jliu.message.BaseMessage;
import nl.tno.stormcv.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * send: "start,[videoAddress]" ---> received:
 * "succeed,[jpeg stream ip address and port] <strong>OR</strong> Error,[reason]"
 * <br>
 * send: "startByMaster,[videoAddress]" ---> received:
 * "succeed,[jpeg stream ip address and port] <strong>OR</strong> Error,[reason]"
 * <br>
 * send: "end,[videoAddress]" ---> received "Succeed" <strong>OR</strong>
 * "Error,[reason]" <br>
 * send: "endByMaster,[videoAddress]" ---> received "Succeed"
 * <strong>OR</strong> "Error,[reason]"
 *
 * @author lwang
 */
public class TCPClient implements Serializable {

    private static final long serialVersionUID = -2854001775235980342L;

    private static Logger logger = LoggerFactory.getLogger(TCPClient.class);

    private Socket socket;
    private String serverIp;
    private int port;

    public TCPClient() {

    }

    public TCPClient(String serverIp, int port) {
        try {
            socket = new Socket(serverIp, port);
            this.serverIp = serverIp;
            this.port = port;
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.info(e.getMessage(), e);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.info(e.getMessage(), e);
        }
    }

    public void sendStreamIdMsg(BaseMessage streamIdMsg) {
        if (null == socket) {
            try {
                socket = new Socket(this.serverIp, this.port);
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.info(e.getMessage(), e);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.info(e.getMessage(), e);
            }
        }

        try {
            OutputStream os = socket.getOutputStream();
            ObjectOutputStream objOs = new ObjectOutputStream(os);
            objOs.writeObject(streamIdMsg);

//			objOs.flush();
//			objOs.close();
//			os.flush();
//			os.close();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.info(e.getMessage(), e);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.info(e.getMessage(), e);
        }
    }

    /**
     * send message to certain server
     *
     * @param msg        message ready to send to server
     * @param serverAddr server ip address
     * @param port       server port
     * @return server replay
     */
//	public String sendMsg(String msg, String serverAddr, int port) {
//		String receivedMsg = "";
//		try {
//			socket = new Socket(serverAddr, port);
//			byte[] data = msg.getBytes("utf-8");
//
//			BufferedReader in = new BufferedReader(new InputStreamReader(
//					socket.getInputStream()));
//			DataOutputStream out = new DataOutputStream(
//					socket.getOutputStream());
//			out.write(data);
//
//			char[] receivedData = new char[1024];
//			// receive
//
//			int numRead = in.read(receivedData);
//			receivedMsg = new String(receivedData, 0, numRead);
//
//			System.out.println("Client Received:" + receivedMsg);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (socket != null) {
//					socket.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return receivedMsg;
//	}
    public byte[] getBufferedImageBytes(String serverAddr, int port) {
        try {
            InputStream in = socket.getInputStream();

            // get bufferedimage byte array length first
            byte[] buffer = new byte[4];
            int readNum = 0;
            while (readNum < 4) {
                int read = in.read(buffer, 0, 4);
                // logger.info("read result of length info: "+read);
                if (read < 0)
                    break;
                readNum += read;
            }
            int length = MathUtil.byteArrayToInt(buffer);
            // logger.info("Image size: "+length);

            // get bufferedimage bytes
            buffer = new byte[length];
            readNum = 0;
            while (readNum < length) {
                int read = in.read(buffer, readNum, length - readNum);
                // logger.info("read result of data info: "+read);
                if (read < 0)
                    break;
                readNum += read;
            }
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
            logger.info(e.getMessage(), e);
        }
        return null;
    }

}
