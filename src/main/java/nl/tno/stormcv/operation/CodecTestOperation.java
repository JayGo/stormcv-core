package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodecTestOperation implements ISingleInputOperation<Frame> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2277441206189310165L;
	private static final Logger logger = LoggerFactory.getLogger(CodecTestOperation.class);
	private String name;
	private int framNr = 0;
	private FrameSerializer serializer = new FrameSerializer();
	
	public CodecTestOperation() {
		
	}
	
	public CodecTestOperation(String name) {
		this.name = name;
	}

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
		return null;
	}

	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return this.getClass().getSimpleName();
	}
	

	@Override
	public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
		Frame sf = (Frame) particle;

		// Decode callback method.
		Mat input = codecHandler.getMat();
		Mat output = input;
		// For decode verification
		Highgui.imwrite("/root/Pictures/"+ ++ framNr +".jpg", output);

		// Encode callback method.
//		byte[] encodedData = codecHandler.encode(output);
//		sf.swapImageBytes(encodedData);
		
		List<Frame> results = new ArrayList<Frame>();
		
		results.add(sf);

		return results;
	}

}
