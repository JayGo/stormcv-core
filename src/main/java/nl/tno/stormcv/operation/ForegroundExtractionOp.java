package nl.tno.stormcv.operation;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;
import nl.tno.stormcv.util.ForegroundExtractionJNI;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.NativeUtils;

/**
 * A simple operation that extract foreground of the image within received {@link Frame} objects to gray
 * using jliu's Cplusplus function. You need load the generated dynamic link file. 
 * 
 * @author jkyan,jliu
 *
 */
public class ForegroundExtractionOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = -9064354876041530640L;
	private Logger logger = LoggerFactory.getLogger(ForegroundExtractionOp.class);
	private FrameSerializer serializer = new FrameSerializer();
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws RuntimeException, IOException {
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
		String foreground_lib = "foreground_extraction.so";
		try {
			System.load(userDir+"/"+foreground_lib);
		} catch (UnsatisfiedLinkError e) {
			logger.error("Cannot load foreground so library!");
		}
		NativeUtils.load();
	}

	@Override
	public List<Frame> execute(CVParticle particle) throws IOException {
		List<Frame> result = new ArrayList<Frame>();
		Frame sf = (Frame) particle;
		if (sf == null || sf.getImage() == null)
			return result;
		BufferedImage image = sf.getImage();
		int height = image.getHeight();
		int width = image.getWidth();

		byte[] initBytes = ImageUtils.imageToBytesWithOpenCV(image);
		if (initBytes == null) return result;
		
		byte[] processedBytes = null;
		processedBytes = ForegroundExtractionJNI.foregroundExtract(initBytes,
				height, width);
		if (processedBytes == null) return result; 
		
		BufferedImage bufferedImage = ImageUtils.bytesToImage(processedBytes);
		
		sf.setImage(bufferedImage);

		result.add(sf);
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
