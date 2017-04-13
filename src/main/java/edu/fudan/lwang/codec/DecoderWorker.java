package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.stormcv.util.TimeElasper;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DecoderWorker extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(DecoderWorker.class);

    private String decoderId;
    private SourceInfo mSourceInfo;
    private CodecType codecType;
    private DecoderCallback mDecoderCallback;
    private int yuvFrameWidth;
    private int yuvFrameHeight;
    private int yuvMatType;
    private byte[] encodedData;
    
    public DecoderWorker(SourceInfo sourceInfo, DecoderCallback decoderCallback) {
    	this.mSourceInfo = sourceInfo;
    	this.codecType = mSourceInfo.getCodecType();
        this.decoderId = mSourceInfo.getSourceId();
        this.yuvFrameWidth = mSourceInfo.getYuvFrameWidth();
        this.yuvFrameHeight = mSourceInfo.getYuvFrameHeight();
        this.yuvMatType = mSourceInfo.getYuvMatType();
    	this.mDecoderCallback = decoderCallback;
    }

    public String getDecoderId() {
        return this.decoderId;
    }

    public int registerDecoder() {
        int codecTypeInt = CodecHelper.getInstance().getCodecTypeInt(codecType);
        Mat pMat = new Mat();
        return CodecHelper.getInstance().registerDecoder(decoderId, codecTypeInt, yuvFrameWidth, yuvFrameHeight, pMat.nativeObj);
    }

    public void releaseDecoder() {
        CodecHelper.getInstance().releaseDecoder(decoderId);
    }

    public void swapEncodedData(byte[] data) {
        this.encodedData = data;
    }

    @Override
    public void run() {
        
        logger.info("Decoder run1!!!!");
        
        int[] results = new int[2];
        Mat yuvMat = new Mat(yuvFrameHeight, yuvFrameWidth, yuvMatType, new Scalar(0));

        TimeElasper timeElasper = new TimeElasper();
        int frameNr = 0;
        
        logger.info("Decoder worker starts to run!");

        while (!Thread.interrupted()) {
//            logger.info("Decoder running!");
            encodedData = mDecoderCallback.getEncodedData();
            if (encodedData != null && encodedData.length > 0) {
//				logger.info("Decoding thread info: [decoderId: "+decoderId+", size: "+encodedData.length);
                // logger.info("======================== decode start ==============================");
                long start = System.currentTimeMillis();
                CodecHelper.getInstance()
                        .decodeFrame(decoderId, encodedData, results, yuvMat.nativeObj);
                long end = System.currentTimeMillis();
                timeElasper.push((int) (end - start));
                
        		if(frameNr == 100 || frameNr == 500 || frameNr == 900
        				|| frameNr == 1300 || frameNr == 1700 || frameNr == 2100 || frameNr == 2500) {
        			logger.info("Decoder on "+decoderId+": Top "+frameNr+"'s time average cost: "+timeElasper.getKAve(frameNr));
        		}

                if (results[0] != 0) {
                    frameNr++;
                    // logger.info("The yuvMat size: "+ yuvMat.width() +"x"+yuvMat.height());
                }

                //System.out.println("YUV mat is null ? " + ((yuvMat == null) ? "yes" : "no"));

                // The yuvMat is complete
                mDecoderCallback.onDataDecoded(yuvMat, results);

            }
        }
    }
}
