package nl.tno.stormcv.operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.util.StreamerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H264RtmpStreamOp implements ISingleInputOperation<Frame> {

	
	private final static Logger logger = LoggerFactory.getLogger(H264RtmpStreamOp.class);
	private FrameSerializer serializer = new FrameSerializer();
	
	private String url;
	private String appName;
	private double frameRate = 25.0;
	private StreamerHelper sh;
	private int frameNr = 0;
	
	private static final String H264Dir = "/root/H264Data/";
	
	public H264RtmpStreamOp RTMPServer(String url) {
		this.url = url;
		return this;
	}

	public H264RtmpStreamOp appName(String appName) {
		this.appName = appName;
		return this;
	}
	
	public H264RtmpStreamOp frameRate(double frameRate) {
		this.frameRate = frameRate;
		return this;
	}
	
	private void initStreamer() {
		String rtmpAddr = url+"/"+appName;
		sh = StreamerHelper.getInstance();
		if(sh.connectToRtmp(rtmpAddr)) {
			logger.info("Conneted to rtmp server: "+rtmpAddr);
		} else {
			logger.error("Connect to rtmp server: "+rtmpAddr +" failed!");
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws Exception {
		// TODO Auto-generated method stub
		initStreamer();
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		// TODO Auto-generated method stub
		return serializer;
	}


	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return this.getClass().getSimpleName();
	}

	public void printData(Frame frame) {
		byte[] datas = frame.getImageBytes();
		int length = datas.length;
		
		if(length <= 0)
			return;

		File dir = new File(H264Dir);
		if (!dir.exists() && !dir.isDirectory()) {
			if (!dir.mkdir()) {
				logger.error("Directory " + dir.getPath() + " create failed!");
			}
		}

		File file = new File(H264Dir + frameNr++);
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}

		if (fos == null) {
			logger.error("Null poniter exception!");
		}
		try {
			for (int i = 0; i < length; i++) {
				fos.write((datas[i] & 0x0FF));
				// logger.info(i+"'th byte value: "+(int) (datas[i]&0x0FF));
			}
			fos.flush();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public List<Frame> execute(CVParticle particle, OperationHandler operationHandler) throws Exception {
		// TODO Auto-generated method stub
		Frame frame = (Frame) particle;
//		logger.info("Receive frame: " + frame);
		
		// printData(frame);

		byte [] frameBytes = frame.getImageBytes();
		if(frameBytes.length >0) {
			if(frameBytes[4] == 103) {
				sh.sendFirstFrame(frameBytes);
			} else {
				sh.sendNormalFrame(frameBytes);
			}
//			logger.info("Emmit frame to rtmp: "+frame);
		} else {
			logger.info("Frame is empty, ignore: "+frame);
		}

		List<Frame> results = new ArrayList<>();
		results.add(frame);
		return results;
	}

}
