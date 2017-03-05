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
        this.frameHeight = (int) (frameHeight * 1.5);
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

    public void swapEncodedData(byte[] data) {
        this.encodedData = data;
    }

    @Override
    public void run() {

        int[] results = new int[2];
        Mat yuvMat = new Mat((int) 1.5 * frameHeight, frameWidth, frameType, new Scalar(0));

        TimeElasper timeElasper = new TimeElasper();
        int frameNr = 0;

        while (!Thread.interrupted()) {
            encodedData = mDecoderCallback.getEncodedData();
            if (encodedData != null && encodedData.length > 0) {
//				logger.info("Decoding thread info: [decoderId: "+decoderId+", size: "+encodedData.length);
                // logger.info("======================== decode start ==============================");
                long start = System.currentTimeMillis();
                CodecHelper.getInstance()
                        .decodeFrame(decoderId, encodedData, results, yuvMat.nativeObj);
                long end = System.currentTimeMillis();
                timeElasper.push((int) (end - start));

//                if (frameNr == 100) {
//                    logger.info("Top {}'s time average cost: {}ms", frameNr, timeElasper.getKAve(100));
//                }
//
//                if (frameNr == 2000) {
//                    logger.info("Top {}'s time average cost: {}ms", frameNr, timeElasper.getKAve(2000));
//                }
                
                if(frameNr%100==0) {
                	logger.info("Top {}'s time average cost: {}ms", frameNr, timeElasper.getKAve(frameNr));
                }
                
                // logger.info("decode results: "+results[0]+", "+results[1]);
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
