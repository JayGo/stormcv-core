package nl.tno.stormcv.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import edu.fudan.lwang.codec.OperationHandler;


import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.util.ImageUtils;

public class CannyEdgeOp extends OpenCVOp<CVParticle> implements
		ISingleInputOperation<CVParticle> {

	private static final long serialVersionUID = -1332602364774824110L;
	@SuppressWarnings("unused")
	private Logger logger = LoggerFactory.getLogger(GrayscaleOp.class);
	@SuppressWarnings("rawtypes")
	private CVParticleSerializer serializer = new FrameSerializer();
	@SuppressWarnings("unused")
	private String name;

	public CannyEdgeOp() {
		
	}
	
	public CannyEdgeOp(String name) {
		this.name = name;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map stormConf, TopologyContext context)
			throws Exception {

	}

	@Override
	public List<CVParticle> execute(CVParticle particle) throws IOException {

		Frame sf = (Frame) particle;

		if (sf.getImage() != null) {
			System.out.println("receive a image...");
		}

		MatOfByte mob = new MatOfByte(sf.getImageBytes());

		Mat input = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);

		Mat inputGray = new Mat();
		Imgproc.cvtColor(input, inputGray, Imgproc.COLOR_BGR2GRAY);

		Mat output = new Mat();
		List<CVParticle> results = new ArrayList<CVParticle>();

		Imgproc.Canny(inputGray, output, 255, 500);

		Mat output3Bytes = new Mat();
		Imgproc.cvtColor(output, output3Bytes, Imgproc.COLOR_GRAY2BGR);

		// sf.setImage(matToBufferedImage(output3Bytes));

		sf.setImage(ImageUtils.matToBufferedImage(output3Bytes));

		results.add(sf);

		return results;
	}

	@Override
	public void deactivate() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return serializer;
	}


	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	// Work with codecHandler callback
	@Override
	public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws IOException {
		// TODO Auto-generated method stub
		Frame sf = (Frame) particle;

		if (sf.getImage() != null) {
			System.out.println("receive a image...");
		}

//		MatOfByte mob = new MatOfByte(sf.getImageBytes());
//
//		Mat input = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
		
		// Decoded callback method.
		Mat input = codecHandler.getMat();

		Mat inputGray = new Mat();
		Imgproc.cvtColor(input, inputGray, Imgproc.COLOR_BGR2GRAY);

		Mat output = new Mat();
		List<CVParticle> results = new ArrayList<CVParticle>();

		Imgproc.Canny(inputGray, output, 255, 500);

		Mat output3Bytes = new Mat();
		Imgproc.cvtColor(output, output3Bytes, Imgproc.COLOR_GRAY2BGR);


		// sf.setImage(ImageUtils.matToBufferedImage(output3Bytes));
		
		// Encode callback method.
		// byte[] encodedData = codecHandler.encode(output3Bytes);
		// sf.swapImageBytes(encodedData);

		results.add(sf);

		return results;
	}



}
