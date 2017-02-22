package nl.tno.stormcv.operation;

import java.awt.image.ColorConvertOp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import edu.fudan.lwang.codec.OperationHandler;

import nl.tno.stormcv.model.Frame;

import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;

/**
 * A simple operation that converts the image within received {@link Frame} objects to gray
 * using Java's native {@link ColorConvertOp}. Hence the OpenCV library is not required to use this Operation.
 * 
 * @author Corne Versloot
 *
 */
public class GrayscaleOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = 1254502507730636800L;
	@SuppressWarnings("unused")
	private Logger logger = LoggerFactory.getLogger(GrayscaleOp.class);
	private FrameSerializer serializer = new FrameSerializer();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) { }

	@Override
	public List<Frame> execute(CVParticle particle) throws IOException {
		List<Frame> result = new ArrayList<Frame>();
		Frame frame = (Frame)particle;
		Mat in = ImageUtils.bufferedImageToMat(frame.getImage());
		
		Mat out = new Mat(in.height(), in.width(), CvType.CV_8UC1);
		Imgproc.cvtColor(in, out, Imgproc.COLOR_BGR2GRAY);

//		BufferedImage bf = ImageUtils.matToBufferedImage(out);				
//		frame.setImage(bf);
		byte[] bufferedImageBytes = ImageUtils.matToBytes(out, frame.getImageType());
		frame.setImage(bufferedImageBytes, frame.getImageType());
		
//		if (frame.getSequenceNr() < 50) {
//			File file = new File("image" + frame.getSequenceNr() + ".jpg");
//			byte[] imageBytes =  frame.getImageBytes();		
//			BufferedImage nowImage = ImageUtils.bytesToImage(imageBytes);
//			ImageIO.write(nowImage, "jpg", file);
//		}
		
		result.add(frame);
		return result;
	}

	@Override
	public void deactivate() { }

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}


	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}




}
