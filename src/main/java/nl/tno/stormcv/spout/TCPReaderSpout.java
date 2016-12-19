package nl.tno.stormcv.spout;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import javax.imageio.ImageIO;

import nl.tno.stormcv.constant.RequestCode;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.service.TCPClient;
import nl.tno.stormcv.util.ImageUtils;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import edu.fudan.jliu.message.BaseMessage;

/**
 * 
 * @author lwang
 *
 */
public class TCPReaderSpout implements IRichSpout {

	private static final Logger logger = Logger.getLogger(TCPReaderSpout.class);
	private static final long serialVersionUID = 7340743805719206817L;
	private SpoutOutputCollector collector;
	private FrameSerializer serializer;

	private String serverIp;
	private int port;
	private String streamId;
	private TCPClient mTCPClient;

	private long frameNr;

	public TCPReaderSpout(String streamId, String serverIp, int port) {
		serializer = new FrameSerializer();
		this.serverIp = serverIp;
		this.port = port;
		this.streamId = streamId;
		frameNr = 0;
	}

	@Override
	public void ack(Object paramObject) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object paramObject) {
		// TODO Auto-generated method stub
		logger.info(new Date().toString() + " Failed. Object " + paramObject);
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		byte[] buffer = null;
		while (buffer == null) {
			buffer = mTCPClient.getBufferedImageBytes(serverIp, port);
		}
		long timestamp = System.currentTimeMillis();
		BufferedImage image;
		try {
			image = ImageUtils.bytesToImage(buffer);
			if(image == null) {
				return;
			}
			// logger.info("frame " + frameNr +  " " + image.getWidth() + "x" + image.getHeight());
			
			// ImageIO.write(image, "JPG", new File("/root/pics/"+frameNr+".jpg"));
			
			Frame newFrame = new Frame(streamId + "", frameNr, Frame.JPG_IMAGE, image,
					timestamp, new Rectangle(0, 0, image.getWidth(),
							image.getHeight()));
			frameNr++;
//			collector.emit(streamId, serializer.toTuple(newFrame));
			collector.emit(serializer.toTuple(newFrame), streamId + "_" + frameNr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map paramMap, TopologyContext paramTopologyContext,
			SpoutOutputCollector paramSpoutOutputCollector) {
		// TODO Auto-generated method stub
		collector = paramSpoutOutputCollector;
		mTCPClient = new TCPClient(serverIp, port);
		BaseMessage streamIdMsg = new BaseMessage(RequestCode.DEFAULT);
		streamIdMsg.setStreamId(streamId);
		mTCPClient.sendStreamIdMsg(streamIdMsg);
		logger.info("streamId message is sent out: "+streamId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(serializer.getFields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
