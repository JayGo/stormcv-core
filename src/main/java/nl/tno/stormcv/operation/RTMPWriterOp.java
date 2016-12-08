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

import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;
import nl.tno.stormcv.util.Constant;

/**
 *
 * @author jkyan
 *
 */

public class RTMPWriterOp implements IBatchOperation<Frame> {

	private static final long serialVersionUID = 3591538303646724289L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String url = Constant.DefaulteRTMPServer;
	private String appName = "";
	private int height = 0;
	private int width = 0;
	private IStreamCoder coder = null;
	private IContainer container = null;
	private Boolean isCoderInit = false;
	private double frameRate = 0.0;

	public RTMPWriterOp RTMPServer(String url) {
		this.url = url;
		return this;
	}

	public RTMPWriterOp appName(String appname) {
		this.appName = appname;
		return this;
	}

	public RTMPWriterOp frameRate(double frameRate) {
		this.frameRate = frameRate;
		return this;
	}

	/**
	 * Sets the classes to be used as resources for this application
	 */
	public Set<Class<?>> getClasses() {
		Set<Class<?>> s = new HashSet<Class<?>>();
		s.add(RTMPWriterOp.class);
		return s;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context)
			throws Exception {
		initCoder();
	}

	@Override
	public void deactivate() {
		coder.close();
		container.close();
	}
	
	public void initCoder() throws Exception {
		if (url == "" || appName == "")
			logger.error("no rtmp server defined!");
		container = IContainer.make();
		IContainerFormat containerFormat = IContainerFormat.make();
		containerFormat.setOutputFormat("flv", url + appName, null);
		// set the buffer length xuggle will suggest to ffmpeg for reading
		// inputs
		container.setInputBufferLength(0);
		int retVal = container.open(url + appName, IContainer.Type.WRITE,
				containerFormat);
		if (retVal < 0) {
			logger.error("Could not open output container for live stream");
		} else {
			logger.info("hava opened server " + url + appName + " for write!");
		}

		// ICodec codec = ICodec.findEncodingCodec(ICodec.ID.CODEC_ID_H264);
		ICodec codec = ICodec.findEncodingCodec(ICodec.ID.CODEC_ID_FLV1);

		if (true || codec == null) {
			//logger.warn("cannot find h264 encoding codec!");
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
		if (coder == null) {
			throw new Exception("[ERROR]rtmp stream coder is null. cannot write rtmp stream.");
		}
		coder.setNumPicturesInGroupOfPictures(30);
		coder.setBitRate(256000);
		coder.setCodec(codec);
		coder.setPixelType(IPixelFormat.Type.YUV420P);
		isCoderInit = false;
	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return new FrameSerializer();
	}

	@Override
	public List<Frame> execute(List<CVParticle> input) throws Exception {
		// logger.info("receive a input in rtmp + size:" + input.size());
		List<Frame> result = new ArrayList<Frame>();
		for (int i = 0; i < input.size(); i++) {
			CVParticle s = input.get(i);
			if (!(s instanceof Frame)) {
				continue;
			}
			Frame frame = (Frame) s;
			result.add(frame);
			BufferedImage image = frame.getImage();
			if (image == null) {
				logger.info("image null");
				continue;
			}
			if (!container.isOpened()) {
				logger.info("The container of the rtmp server unexcepect closed!");
				initCoder();	
				continue;
			}
			if (!isCoderInit) {
				width = image.getWidth();
				height = image.getHeight();
				if (width == 0 || height == 0) {
					logger.error("cannot get the real size of the stream needed to read");
				} else {
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
				} else {
					logger.info("write header success");
				}
				isCoderInit = true;
				logger.info("coder init finished!");
			}

			BufferedImage convertedImage = new BufferedImage(image.getWidth(),
					image.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
			convertedImage.getGraphics().drawImage(image, 0, 0, null);

			IConverter converter = ConverterFactory.createConverter(
					convertedImage, IPixelFormat.Type.YUV420P);
			
			
			IVideoPicture outFrame = converter.toPicture(convertedImage,
					frame.getTimestamp() * 1000);
			if (frame.getSequenceNr() == 0) {
				outFrame.setKeyFrame(true);
			}
			IPacket packet = IPacket.make();
			outFrame.setQuality(0);
			if (coder.encodeVideo(packet, outFrame, 0) < 0) {
				logger.error("encode falied");
			}
			outFrame.delete();

			if (packet.isComplete()) {				
				if (container.writePacket(packet, true) < 0) {
					throw new RuntimeException("cannot write packet");
				} else {
					//logger.info("write a package of size " + packet.getSize());
				}
			}
		}
		return result;
	}
}
