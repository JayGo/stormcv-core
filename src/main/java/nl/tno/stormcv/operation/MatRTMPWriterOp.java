package nl.tno.stormcv.operation;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IRational;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.video.ConverterFactory;
import com.xuggle.xuggler.video.IConverter;
import com.xuggle.xuggler.video.ConverterFactory.Type;

import edu.fudan.lwang.converter.GrayConverter;
import nl.tno.stormcv.model.MatImage;
import nl.tno.stormcv.model.serializer.MatImageSerializer;
import nl.tno.stormcv.constant.ZKConstant;
import nl.tno.stormcv.util.ImageUtils;

public class MatRTMPWriterOp implements IMatOperation<MatImage> {

	private static final long serialVersionUID = 6453978086650507695L;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String url = ZKConstant.DefaultRTMPServer;
	private String appName = "";
	private int height = 0;
	private int width = 0;
	private IStreamCoder coder = null;
	private IContainer container = null;
	private Boolean isCoderInit = false;
	private double frameRate = 0.0;
	
	private IContainerFormat containerFormat;

	public MatRTMPWriterOp RTMPServer(String url) {
		this.url = url;
		return this;
	}

	public MatRTMPWriterOp appName(String appname) {
		this.appName = appname;
		return this;
	}

	public MatRTMPWriterOp frameRate(double frameRate) {
		this.frameRate = frameRate;
		return this;
	}
	
	public Set<Class<?>> getClasses() {
		Set<Class<?>> s = new HashSet<Class<?>>();
		s.add(MatRTMPWriterOp.class);
		return s;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context)
			throws Exception {
		// TODO Auto-generated method stub
		initCoder();
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		container.delete();
		coder.close();
		container.close();
	}

	@Override
	public MatImageSerializer getSerializer() {
		// TODO Auto-generated method stub
		return new MatImageSerializer();
	}

	@Override
	public List<MatImage> execute(MatImage image) throws Exception {
		// TODO Auto-generated method stub
		List<MatImage> result = new ArrayList<MatImage>();
		
		if(image.getMat() == null || image.getMat().empty()) {
			return result;
		}
		
		BufferedImage bfImage = ImageUtils.matToBufferedImage(image.getMat());

//		if (frame.getSequenceNr() < 400) {
//			File file = new File("/home/lwang/workspace/image" + frame.getSequenceNr() + ".jpg");
//			ImageIO.write(image, "jpg", file);
//		}
		
		if (bfImage == null) {
			logger.error("image null");
			return result;
		}
		if (container == null || !container.isOpened()) {
			logger.error("The container of the rtmp server unexcepect closed!");
			initCoder();
		}

		if (!isCoderInit) {
			width = image.getWidth();
			height = image.getHeight();
			if (width == 0 || height == 0) {
				logger.error("cannot get the real size of the stream needed to read");
			}
			else {
				coder.setHeight(height);
				coder.setWidth(width);
			}

			coder.setFlag(IStreamCoder.Flags.FLAG_QSCALE, true);
			coder.setGlobalQuality(0);
			IRational rationalFrameRate = IRational.make(frameRate);
			coder.setFrameRate(rationalFrameRate);
			coder.setTimeBase(IRational.make(
					rationalFrameRate.getDenominator(),
					rationalFrameRate.getNumerator()));
			coder.open(null, null);
			if (container.writeHeader() < 0) {
				throw new RuntimeException("cannot write header");
			}
			else {
				logger.info("write header success");
			}
			isCoderInit = true;
			logger.info("coder init finished!");
		}

//		 System.out.println("Frame " + frame.getSequenceNr() + " " +
//		 image.getWidth() + "x" + image.getHeight() + " " + image.getType());

		BufferedImage convertedImage = new BufferedImage(bfImage.getWidth(),
				bfImage.getHeight(), bfImage.getType());
		convertedImage.getGraphics().drawImage(bfImage, 0, 0, null);

		IConverter converter = ConverterFactory.createConverter(convertedImage,
				IPixelFormat.Type.YUV420P);

		IVideoPicture outFrame = converter.toPicture(convertedImage,
				image.getTimestamp() * 1000);
		if (image.getSequence() == 0) {
			outFrame.setKeyFrame(true);
		}
		IPacket packet = IPacket.make();
		outFrame.setQuality(0);
		if (coder.encodeVideo(packet, outFrame, 0) < 0) {
			logger.error("encode falied");
		}
		outFrame.delete();

		if (packet.isComplete()) {
			if (container.writePacket(packet) < 0) {
				container.open(url + appName, IContainer.Type.WRITE,
						containerFormat);
//				throw new RuntimeException("cannot write packet");
			}
			else {
				// logger.info("write a package of size " + packet.getSize());
			}
		}
		return result;
	}
	
	public void initCoder() throws Exception {
		if (url == "" || appName == "")
			logger.error("no rtmp server defined!");
		container = IContainer.make();
		containerFormat = IContainerFormat.make();
		containerFormat.setOutputFormat("flv", url + appName, null);
		// set the buffer length xuggle will suggest to ffmpeg for reading
		// inputs
		container.setInputBufferLength(0);
		int retVal = container.open(url + appName, IContainer.Type.WRITE,
				containerFormat);
		if (retVal < 0) {
			logger.error("Could not open output container for live stream");
		}
		else {
			logger.info("hava opened server " + url + appName + " for write!");
		}

		// ICodec codec = ICodec.findEncodingCodec(ICodec.ID.CODEC_ID_H264);
		ICodec codec = ICodec.findEncodingCodec(ICodec.ID.CODEC_ID_FLV1);

		if (false || codec == null) {
			// logger.warn("cannot find h264 encoding codec!");
			Collection<ICodec> icodec_collections = ICodec.getInstalledCodecs();
			Iterator<ICodec> iterator = icodec_collections.iterator();
			while (iterator.hasNext()) {
				ICodec icodec = iterator.next();
				logger.info("Your system supports codec:" + icodec.getName());
			}
		}

		// container.setForcedVideoCodec(ICodec.ID.CODEC_ID_H264);
		// container.setForcedVideoCodec(ID.CODEC_ID_MPEG4);

		IStream stream = container.addNewStream(codec);
		coder = stream.getStreamCoder();
		if (codec != null) {
			coder.setNumPicturesInGroupOfPictures(30);
			coder.setBitRate(256000);
			coder.setCodec(codec);
			coder.setPixelType(IPixelFormat.Type.YUV420P);
		}
		else {
			throw new Exception(
					"[ERROR]rtmp stream coder is null. cannot write rtmp stream.");
		}
		isCoderInit = false;
		ConverterFactory.registerConverter(new Type("LWANG-GRAY-8", GrayConverter.class,
				IPixelFormat.Type.GRAY8, 10));
	}

}
