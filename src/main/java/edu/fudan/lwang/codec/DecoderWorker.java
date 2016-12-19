package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

import org.apache.log4j.Logger;
import org.apache.storm.generated.DistributedRPCInvocations.AsyncProcessor.result;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;


public class DecoderWorker extends Thread {

	private final static Logger logger = Logger.getLogger(DecoderWorker.class);
	
	private String decoderId;
	private CodecType codecType;
	private DecoderCallback mDecoderCallback;
	private int frameWidth;
	private int frameHeight;
	private int frameType;
	private byte[] encodedData;
	
	public DecoderWorker(String decoderId, CodecType codecType, 
			int frameWidth, int frameHeight, int frameType, DecoderCallback mDecoderCallback) {
		this.decoderId = decoderId;
		this.codecType = codecType;
		this.frameWidth = frameWidth;
		this.frameHeight = frameHeight;
		this.frameType = frameType;
		this.mDecoderCallback = mDecoderCallback;
	}
	
	public DecoderWorker(CodecType codecType) {
		this.codecType = codecType;
	}
	
	public DecoderWorker build(String decoderId, int frameWidth, int frameHeight, DecoderCallback mDecoderCallback) {
		this.decoderId = decoderId;
		this.frameWidth = frameWidth;
		this.frameHeight = (int) (frameHeight*1.5);
		this.mDecoderCallback = mDecoderCallback;
		return this;
	}
	
	public String getDecoderId() {
		return this.decoderId;
	}
	
	public int registerDecoder() {
		int codecTypeInt = CodecHelper.getInstance().getCodecTypeInt(codecType);
		return CodecHelper.getInstance().registerDecoder(decoderId, codecTypeInt);
	}
	
	public void releaseDecoder() {
		CodecHelper.getInstance().releaseDecoder(decoderId);
	}
	
	public void swapEncodedData(byte [] data) {
		this.encodedData = data;
	}
	
	@Override
	public void run() {
		
		int[] results = new int[2];
		Mat yuvMat = new Mat((int)1.5*frameHeight, frameWidth, frameType, new Scalar(0));
		
		while(!Thread.interrupted()) {
			encodedData = mDecoderCallback.getEncodedData();
			if(encodedData != null) {
//				logger.info("Decoding thread info: [decoderId: "+decoderId+", size: "+encodedData.length);
				
				CodecHelper.getInstance()
						.decodeFrame(decoderId, encodedData, results, yuvMat.nativeObj);
				
				// logger.info("decode results: "+results[0]+", "+results[1]);
				if(results[0] != 0) {
					logger.info("The yuvMat size: "+ yuvMat.width() +"x"+yuvMat.height());
				}
				
				// The yuvMat is complete
				mDecoderCallback.onDataDecoded(yuvMat, results[1]);

			}
		}
	}
}
