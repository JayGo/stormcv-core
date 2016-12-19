package nl.tno.stormcv.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.NativeUtils;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.opencv.highgui.Highgui;

import edu.fudan.lwang.FGEHelper;
import edu.fudan.lwang.codec.OperationHandler;

/**
 * 
 * @author lwang
 *
 */
public class FGExtranctionOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = -2650744876409977812L;
	private FrameSerializer serializer = new FrameSerializer();
	private static FGEHelper mFGEHelper = new FGEHelper();
//	private Logger logger = LoggerFactory.getLogger(FGExtranctionOp.class);
	private boolean isLoadLibrary = true;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context)
			throws Exception {
		// TODO Auto-generated method stub
		isLoadLibrary = NativeUtils.loadCudaLibrary() &&  NativeUtils.loadBayesFGELibrary();
//		System.out.println("load library result : " + NativeUtils.loadCudaLibrary() + "/"  +  NativeUtils.loadBayesFGELibrary());
		if(isLoadLibrary) {
			mFGEHelper.init0(1920, 1080, 3);
		}
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
		List<Frame> result = new ArrayList<Frame>();
		Frame frame = (Frame)particle;
		Mat in = ImageUtils.bufferedImageToMat(frame.getImage());
		
//		System.out.print("in image " + frame.getSequenceNr() + " : " + in.width() + "x" + in.height() + " dims " + in.dims());
		
		Mat out = null;
		if(isLoadLibrary) {
			out = new Mat(new FGEHelper().getFGImage(in.nativeObj));
		}
		if(out == null) {
			return result;
		}
//		System.out.print("out image " + frame.getSequenceNr() + " : " + out.width() + "x" + out.height() + " dims " + out.dims());

//		if(frame.getSequenceNr() % 100 == 0 && frame.getSequenceNr() <= 1000) {
//		if(frame.getSequenceNr() <= 400) {
//			Highgui.imwrite("/home/lwang/workspace/solved" + frame.getSequenceNr() + ".jpg", out);
//		}

		byte[] bufferedImageBytes = ImageUtils.matToBytes(out, frame.getImageType());
		frame.setImage(bufferedImageBytes, frame.getImageType());
//		BufferedImage bf = ImageUtils.matToBufferedImage(out);
//		frame.setImage(bf);
//		out.release();
		
		result.add(frame);
		return result;
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
