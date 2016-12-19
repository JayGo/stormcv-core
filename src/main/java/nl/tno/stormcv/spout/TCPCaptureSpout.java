package nl.tno.stormcv.spout;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.service.TCPClient;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;

import edu.fudan.lwang.codec.Codec;
import edu.fudan.lwang.codec.MatQueueManager;
import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;

/**
 * 
 * @author lwang
 *
 */
public class TCPCaptureSpout implements IRichSpout {

	private static final Logger logger = Logger.getLogger(TCPCaptureSpout.class);
	private static final long serialVersionUID = 7340743805719206817L;
	private SpoutOutputCollector collector;
	private FrameSerializer serializer;

	private String streamId;
	private String videoAddr;

	private long frameNr;
	
	private SourceInfo mSourceInfo;

	public TCPCaptureSpout(String streamId, String videoAddr, CodecType type) {
		// TODO Auto-generated constructor stub
		serializer = new FrameSerializer();
		this.streamId = streamId;
		this.videoAddr = videoAddr;
		frameNr = 0;
		
		mSourceInfo = Codec.fetchSourceInfo(videoAddr, streamId, type);
		
		if(mSourceInfo==null) {
			logger.info("Capture video failed! System exit!");
			System.exit(-1);
		}
		logger.info("Fetch source info succeed, sourceInfo: " + mSourceInfo);
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
//		byte[] buffer = null;
//		// mock the blocking effect
//		while (buffer == null) {
//			buffer = Codec.getEncodeDataFromBuffer(mSourceInfo.getEncodeQueueId());
//		}
//		long timeStamp = System.currentTimeMillis();
//		try {
//			
//			Frame newFrame = new Frame(streamId, frameNr, Frame.X264_Bytes, buffer, timeStamp, 
//					new Rectangle(0, 0, mSourceInfo.getFrameWidth(), mSourceInfo.getFrameHeight()));
//			
//			frameNr++;
//			collector.emit(serializer.toTuple(newFrame), streamId + "_" + frameNr);
//			logger.info("target: "+mSourceInfo+""+frameNr);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map paramMap, TopologyContext paramTopologyContext,
			SpoutOutputCollector paramSpoutOutputCollector) {
		// TODO Auto-generated method stub
		collector = paramSpoutOutputCollector;
		System.load("/usr/local/opencv/share/OpenCV/java/libopencv_java2413.so");
		System.load("/usr/local/LwangCodec/lib/libHgCodec.so");
		logger.info("TCPCaptureSpout ready to start: "+mSourceInfo);
		
		//************* this block is a test of lwang's encoder and decoder ********
		Codec.startEncodeToBuffer(mSourceInfo);
		
		String decoderQueueId = mSourceInfo.getEncodeQueueId()+"_decoder";
		MatQueueManager.getInstance().registerQueue(decoderQueueId);
		Codec.registerDecoder(mSourceInfo, mSourceInfo.getEncodeQueueId(), decoderQueueId);
		// ************************* end of block **********************************
		
		logger.info("TCPCaptureSpout is opened!");
//		BaseMessage streamIdMsg = new BaseMessage(RequestCode.DEFAULT);
//		streamIdMsg.setStreamId(streamId);
//		mTCPClient.sendStreamIdMsg(streamIdMsg);
//		logger.info("streamId message is sent out: "+streamId);
	}
	
	public SourceInfo getSourceInfo() {
		return mSourceInfo;
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
