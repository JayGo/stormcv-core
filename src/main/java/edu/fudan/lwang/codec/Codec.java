package edu.fudan.lwang.codec;

import java.awt.Rectangle;

import org.apache.log4j.Logger;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.opencv.imgproc.Imgproc;
import org.opencv.video.Video;

import com.amazonaws.services.elasticbeanstalk.model.transform.RetrieveEnvironmentInfoResultStaxUnmarshaller;
import com.amazonaws.services.opsworks.model.Source;
import com.amazonaws.services.rds.model.ProvisionedIopsNotAvailableInAZException;
import com.amazonaws.services.simpleworkflow.model.SignalExternalWorkflowExecutionFailedCause;
import com.google.common.base.FinalizablePhantomReference;

import edu.fudan.lwang.codec.Common.CodecType;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.util.HashUtil;
import scala.collection.generic.BitOperations.Int;

public class Codec {
	private static Logger logger = Logger.getLogger(Codec.class);
	private static final int maxEncoderQueueSize = 32*1024*1024 - 1;
	private static final int maxDecoderQueueFrames = 120;
	private static final int ENCODE_DATA_SEG = 2048;
	// private static final FrameQueueManager mFrameQueueManager = FrameQueueManager.getInstance();
	private static final BufferQueueManager mBufferQueueManager = BufferQueueManager.getInstance();
	private static final MatQueueManager mMatQueueManager = MatQueueManager.getInstance();
	private static final CodecManager mCodecManager = CodecManager.getInstance();
	
	public static byte [] getEncodeDataFromBuffer(String queueId) {
		return mBufferQueueManager.getBuffer(queueId, ENCODE_DATA_SEG);
	}
	
	public static SourceInfo fetchSourceInfo(String videoAddr, String streamId, CodecType type) {
		SourceInfo sourceInfo = null;
		VideoCapture videoCapture = new VideoCapture();
		if(!videoCapture.open(videoAddr)) {
			logger.info("VideoCapture open "+videoAddr+" failed!");
			return sourceInfo;
		}
		sourceInfo = new SourceInfo();
		sourceInfo.setFrameWidth((int)videoCapture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH));
		sourceInfo.setFrameHeight((int)videoCapture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT));
		sourceInfo.setEncodeQueueId(streamId);
		sourceInfo.setType(type);
		sourceInfo.setVideoAddr(videoAddr);
		return sourceInfo;
	}
	
	
	public static void startEncodeToBuffer(SourceInfo si) {
		String encoderId = null;
		String videoAddr = si.getVideoAddr();
		String streamId = si.getEncodeQueueId();
		CodecType type = si.getType();
		final VideoCapture capture  = new VideoCapture();
		
		if(!capture.open(videoAddr)) {
			logger.info("Video open failed!");
			return;
		}
		
		encoderId = streamId;
		final String encodeQueueId = encoderId;
		mBufferQueueManager.registerBufferQueue(encodeQueueId, maxEncoderQueueSize);
		
		EncoderWorker encoderWorker = EncoderFactory.create(type).build(videoAddr, encoderId, capture, new EncoderCallback() {
			@Override
			public Mat beforeDataEncoded(Mat frame) {
				// TODO Auto-generated method stub
				Mat yuvMat = new Mat();
				Imgproc.cvtColor(frame, yuvMat, Imgproc.COLOR_BGR2YUV_I420);
				return yuvMat;
			}
			
			@Override
			public void onDataEncoded(byte[] encodedData) {
				// TODO Auto-generated method stub
				while(!mBufferQueueManager.fillBuffer(encodeQueueId, encodedData));
			}

			@Override
			public void onEncoderClosed() {
				// TODO Auto-generated method stub
				
			}
		});
		
		if(null == encoderWorker) {
			logger.info("Unknow encoder type, encoderWorker is null!");
			return;
		}
		
		if(Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
			logger.info("Register encoder for "+encoderWorker.getEncoderId()+" failed!");
			return;
		}
		
		mCodecManager.startEncode(encoderId);
		
	}
	
	public static String registerEncoder(SourceInfo si, String context) {
		final String encodeQueueId = si.getEncodeQueueId()+"_"+context;
		si.setEncodeQueueId(encodeQueueId);
		final CodecType type = si.getType();
		final int frameWidth = si.getFrameWidth();
		final int frameHeight = si.getFrameHeight();
		
		final String encoderId = encodeQueueId;
	
		mBufferQueueManager.registerBufferQueue(encodeQueueId, maxEncoderQueueSize);
		
		EncoderWorker encoderWorker = EncoderFactory.create(type).build(encoderId, frameWidth, frameHeight, new EncoderCallback() {
			@Override
			public Mat beforeDataEncoded(Mat frame) {
				// TODO Auto-generated method stub
				Mat yuvMat = new Mat();
				Imgproc.cvtColor(frame, yuvMat, Imgproc.COLOR_BGR2YUV_I420);
				return yuvMat;
			}
			
			@Override
			public void onDataEncoded(byte[] encodedData) {
				// TODO Auto-generated method stub
				while(!mBufferQueueManager.fillBuffer(encodeQueueId, encodedData));
			}

			@Override
			public void onEncoderClosed() {
				// TODO Auto-generated method stub
			}
		});
		
		if(null == encoderWorker) {
			logger.info("Unknow encoder type, encoderWorker is null!");
			System.exit(1);
			return null;
		}
		
		if(Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
			logger.info("Register encoder for "+encoderWorker.getEncoderId()+" failed!");
			return null;
		}
		
		mCodecManager.startEncode(encoderId);
		
		logger.info("EncodeWoker has started: "+si);
		
		return encodeQueueId;
	}
	
	public static boolean registerDecoder(SourceInfo si, final String sourceQueueId, final String decoderQueueId) {
		
		final CodecType type = si.getType();
		final int frameWidth = si.getFrameWidth();
		final int frameHeight = si.getFrameHeight();
		
		if(mBufferQueueManager.getBufferQueue(sourceQueueId)==null) {
			logger.error("Buffer queue for source "+sourceQueueId+" hasn't register yet!");
			return false;
		}
		
		if(mMatQueueManager.getQueueById(decoderQueueId) == null) {
			logger.error("Mat queued for decoder "+decoderQueueId+" hasn't register yet!");
			return false;
		}
		
		final String decoderId = decoderQueueId;
		
		DecoderWorker decoderWorker = DecoderFactory.create(type).build(decoderId, frameWidth, frameHeight, new DecoderCallback() {
			private int frNum=0;
			@Override
			public byte[] getEncodedData() {
				// TODO Auto-generated method stub
				byte[] encodedData = null;
				do {
					encodedData = mBufferQueueManager.getBuffer(sourceQueueId, ENCODE_DATA_SEG, false);
				}while(encodedData == null || encodedData.length<ENCODE_DATA_SEG);
				return encodedData;
			}
			
			@Override
			public void onDataDecoded(Mat frame, int dataUsed) {
				// TODO Auto-generated method stub
				mBufferQueueManager.moveHeadPtr(sourceQueueId, dataUsed);
				
				Mat rgbFrame = new Mat();
				Imgproc.cvtColor(frame, rgbFrame, Imgproc.COLOR_YUV2BGR_I420);
				Highgui.imwrite("/root/Pictures/"+frNum++ +".jpg", rgbFrame);
//				Highgui.imwrite("/home/jliu/Pictures/"+frNum++ +".jpg", rgbFrame);
				// mMatQueueManager.putElement(decoderQueueId, rgbFrame);
			}
		});
		
		if(Common.CODEC_OK != mCodecManager.registerDecoder(decoderWorker)) {
			logger.error("Register decoder for "+decoderWorker.getDecoderId()+" failed!");
			return false;
		}
		
		mCodecManager.startDecode(decoderId);
		
		logger.info("DecodeWoker has started: "+ decoderId);
		
		return false;
	}
	
}
