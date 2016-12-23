package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import nl.tno.stormcv.util.TimeElasper;

import org.apache.log4j.Logger;
import org.omg.CORBA.PRIVATE_MEMBER;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;

import com.amazonaws.services.elasticache.model.transform.CacheNodeTypeSpecificParameterStaxUnmarshaller;

public class EncoderWorker extends Thread {

	private final static Logger logger = Logger.getLogger(EncoderWorker.class);
	private String videoAddr;
	private String encoderId;
	private CodecType codecType;
	private EncoderCallback mEncoderCallBack;
	private VideoCapture capture;
	private int frameWidth;
	private int frameHeight;
	private Mat frame;
	
	private int frameNr = 0;
	private TimeElasper timeElasper = new TimeElasper();
	
	public EncoderWorker(String encoderId, CodecType codecType, EncoderCallback mEncoderCallBack, VideoCapture capture) {
		this.encoderId = encoderId;
		this.codecType = codecType;
		this.mEncoderCallBack = mEncoderCallBack;
		this.capture = capture;
		frameWidth = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
		frameHeight = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
	}
	
	public EncoderWorker(CodecType codecType) {
		this.codecType = codecType;
	}
	
	// For encoder in spout
	public EncoderWorker build(String videoAddr, String encoderId, VideoCapture capture, EncoderCallback encoderCallBack) {
		this.videoAddr = videoAddr;
		this.encoderId = encoderId;
		this.capture = capture;
		frameWidth = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
		frameHeight = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
		this.mEncoderCallBack = encoderCallBack;
		return this;
	}
	
	// For encoder in bolt
	public EncoderWorker build(String encoderId, int frameWidth, int frameHeight, EncoderCallback encoderCallBack) {
		this.encoderId = encoderId;
		this.frameWidth = frameWidth;
		this.frameHeight = frameHeight;
		this.mEncoderCallBack = encoderCallBack;
		return this;
	}
	
	public String getVideoAddr() {
		return videoAddr;
	}

	public void setVideoAddr(String videoAddr) {
		this.videoAddr = videoAddr;
	}

	public CodecType getCodecType() {
		return codecType;
	}

	public void setCodecType(CodecType codecType) {
		this.codecType = codecType;
	}

	public EncoderCallback getmEncoderCallBack() {
		return mEncoderCallBack;
	}

	public void setmEncoderCallBack(EncoderCallback mEncoderCallBack) {
		this.mEncoderCallBack = mEncoderCallBack;
	}

	public VideoCapture getCapture() {
		return capture;
	}

	public void setCapture(VideoCapture capture) {
		this.capture = capture;
	}

	public int getFrameWidth() {
		return frameWidth;
	}

	public void setFrameWidth(int frameWidth) {
		this.frameWidth = frameWidth;
	}

	public int getFrameHeight() {
		return frameHeight;
	}

	public void setFrameHeight(int frameHeight) {
		this.frameHeight = frameHeight;
	}

	public void setEncoderId(String encoderId) {
		this.encoderId = encoderId;
	}
	
	public String getEncoderId() {
		return this.encoderId;
	}
	
	public int registerEncoder() {
		int codecTypeInt = CodecHelper.getInstance().getCodecTypeInt(codecType);
		return CodecHelper.getInstance().registerEncoder(encoderId, codecTypeInt, frameWidth, frameHeight);
	}
	
	public void releaseEncoder() {
		CodecHelper.getInstance().releaseEncoder(encoderId);
	}
	
	private void processEncoding() {
		Mat processedMat = mEncoderCallBack.beforeDataEncoded(frame);

		long start = System.currentTimeMillis();
		logger.info("encode "+ ++frameNr + "frame started!");
		byte[] encodedData = CodecHelper.getInstance().encodeFrame(encoderId, processedMat.nativeObj);
		long end = System.currentTimeMillis();
		logger.info("encode "+frameNr + "frame done!");
//		timeElasper.push((int)(end-start));
//		
//		if(frameNr == 100) {
//			logger.info("Top "+frameNr+"'s time average cost: "+timeElasper.getKAve(100));
//		}				
//		
//		if(frameNr == 2000) {
//			logger.info("Top "+frameNr+"'s time average cost: "+timeElasper.getKAve(2000));
//		}
		
		logger.info(++frameNr + "frame ecoded data bytes length: "+encodedData.length);
		mEncoderCallBack.onDataEncoded(encodedData);
		logger.info("encode complete!");
	}
	
	@Override
	public void run() {
		frame = null;
		
		if(capture == null) {
			while(!Thread.interrupted()) {
				while((frame = mEncoderCallBack.getDecodedData()) == null);
				processEncoding();
			}
		} else {
			frame = new Mat();
			while(!Thread.interrupted() && (capture != null && capture.open(videoAddr))) {
				if(capture!=null) logger.info("capture is opened for: "+videoAddr);
				while(!Thread.interrupted() && (capture != null && capture.read(frame))) {
					if(!frame.empty()) {
						processEncoding();
					}
				}
			}
		}
		
		releaseEncoder();
		/**
		 * send msg to stop decoder
		 */
	}
	
	
}
