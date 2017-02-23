package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyOperation2 implements ISingleInputOperation<Frame>{

	private static final Logger logger = LoggerFactory.getLogger(EmptyOperation2.class);
	private String name;
	private int frameNr = 0;
	private FrameSerializer serializer = new FrameSerializer();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws Exception {
		// TODO Auto-generated method stub
		
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
		Frame frame = (Frame) particle;
		logger.info("Receive frame: "+frame);
		List<Frame> results = new ArrayList<>();
		results.add(frame);
		return results;
	}

	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return this.getClass().getSimpleName();
	}

	@Override
	public List<Frame> execute(CVParticle particle, OperationHandler operationHandler) throws Exception {
		// TODO Auto-generated method stub
		Frame frame = (Frame) particle;
		logger.info("Receive frame: "+frame);
		operationHandler.fillSourceBufferQueue(frame);
		
		Mat mat = operationHandler.getMat();
		
		if(mat != null) {
			// logger.info("Decode "+frameNr+" done");
			// Highgui.imwrite("/root/Pictures/"+ frameNr++ +".jpg", mat);
			byte [] encodedData = operationHandler.getEncodedData(mat);
			if(encodedData == null) {
				logger.error("encode data is null!!");
			}
			frame.swapImageBytes(encodedData);
		}
		
		List<Frame> results = new ArrayList<>();
		results.add(frame);
		return results;
		

	}

}
