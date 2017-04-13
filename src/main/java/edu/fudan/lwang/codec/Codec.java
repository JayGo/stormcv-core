package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import kafka.serializer.Decoder;

import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.elasticbeanstalk.model.transform.RetrieveEnvironmentInfoResultStaxUnmarshaller;
import com.yammer.metrics.stats.EWMA;

import java.awt.*;

public class Codec {
    private static Logger logger = LoggerFactory.getLogger(Codec.class);
    private static final int maxEncoderQueueSize = 32 * 1024 * 1024 - 1;
    private static final int maxEncoderQueueFrames = 1800;
    private static final int maxDecoderQueueFrames = 120;
    private static final int ENCODE_DATA_SEG = 2048;
    // private static final FrameQueueManager mFrameQueueManager =
    // FrameQueueManager.getInstance();
    private static final BufferQueueManager mBufferQueueManager = BufferQueueManager.getInstance();
    private static final MatQueueManager mMatQueueManager = MatQueueManager.getInstance();
    private static final CodecManager mCodecManager = CodecManager.getInstance();
    private static final FrameQueueManager mFrameQueueManager = FrameQueueManager.getInstance();

    private static Queue<Frame> mEncodedFrameQueue;

    public static byte[] getEncodeDataFromBuffer(String queueId) {
        return mBufferQueueManager.getBuffer(queueId, ENCODE_DATA_SEG);
    }
    
    public static Mat fetchMatSample(String videoAddr, boolean isYUVSample) {
    	Mat sample = new Mat();
    	VideoCapture cap = new VideoCapture();

    	while(!cap.open(videoAddr)) {
    		logger.info("Try to open capture for: "+videoAddr);
    	}

    	while (!cap.read(sample) || sample==null || sample.empty()) {
    		logger.info("Try to read mat sample from "+videoAddr);
		}

    	if(isYUVSample) {
    		Imgproc.cvtColor(sample, sample, Imgproc.COLOR_BGR2YUV_I420);
    	}

    	return sample;
    }

    public static SourceInfo fetchSourceInfo(String videoAddr, String streamId, CodecType type) {
        SourceInfo sourceInfo = null;
        VideoCapture videoCapture = new VideoCapture();
        if (!videoCapture.open(videoAddr)) {
            logger.info("VideoCapture open " + videoAddr + " failed!");
            return sourceInfo;
        }
        sourceInfo = new SourceInfo();
        Mat sample = fetchMatSample(videoAddr, false);
        sourceInfo.setFrameWidth(sample.width());
        sourceInfo.setFrameHeight(sample.height());
        sourceInfo.setMatType(sample.type());
        
        sourceInfo.setSourceId(streamId);
        sourceInfo.setCodecType(type);
        sourceInfo.setVideoAddr(videoAddr);
        
        Mat yuvSample = fetchMatSample(videoAddr, true);
        sourceInfo.setYuvFrameWidth(yuvSample.width());
        sourceInfo.setYuvFrameHeight(yuvSample.height());
        sourceInfo.setYuvMatType(yuvSample.type());
        
        sourceInfo.setCodecType(Common.CodecType.CODEC_TYPE_H264_CPU);
        
        return sourceInfo;
    }
    
    

    public static String startEncodeToBuffer(SourceInfo si) {
        String encoderId = null;
        String videoAddr = si.getVideoAddr();
        String streamId = si.getSourceId();
        CodecType type = si.getCodecType();
        final VideoCapture capture = new VideoCapture();

        if (!capture.open(videoAddr)) {
            logger.info("Video open failed!");
            return null;
        }

        encoderId = streamId;
        final String sourceId = encoderId;
        mBufferQueueManager.registerBufferQueue(sourceId, maxEncoderQueueSize);

        EncoderWorker encoderWorker = EncoderFactory.create(type).build(videoAddr, encoderId, capture, new EncoderCallback() {
            private int frameNr = 0;

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
                if (encodedData.length == 0) return;
                while (!mBufferQueueManager.fillBuffer(sourceId, encodedData)) ;
            }

            @Override
            public void onEncoderClosed() {
                // TODO Auto-generated method stub

            }

            @Override
            public Mat getDecodedData() {
                // TODO Auto-generated method stub
                return null;
            }
        });

        if (null == encoderWorker) {
            logger.info("Unknow encoder type, encoderWorker is null!");
            return null;
        }

        if (Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
            logger.info("Register encoder for " + encoderWorker.getEncoderId() + " failed!");
            return null;
        }

        mCodecManager.startEncode(encoderId);
        return sourceId;
    }
    

    public static String startEncodeToFrameQueue(final SourceInfo si) {
        String encoderId = null;
        String videoAddr = si.getVideoAddr();
        String streamId = si.getSourceId();


        encoderId = streamId;
        final String encodeFrameQueueId = encoderId;

        mFrameQueueManager.registerQueue(encodeFrameQueueId, maxEncoderQueueFrames);
        if ((mEncodedFrameQueue = mFrameQueueManager.getQueueById(encodeFrameQueueId)) == null) {
            logger.info("Register ecodedFrameQueue for " + encodeFrameQueueId + " failed!");
            return null;
        }
        
        EncoderWorker encoderWorker = new EncoderWorker(si, new EncoderCallback() {
            private int frameNr = 0;
            private final Logger logger1 = LoggerFactory.getLogger(EncoderCallback.class);

            @Override
            public Mat beforeDataEncoded(Mat frame) {
                // TODO Auto-generated method stub
                // logger1.info("=================== encode start ==================================");
                Mat yuvMat = new Mat();
                Imgproc.cvtColor(frame, yuvMat, Imgproc.COLOR_BGR2YUV_I420);
                // logger1.info("=================== cvt done ==================================");
                return yuvMat;
            }

            @Override
            public void onDataEncoded(byte[] encodedData) {
                // TODO Auto-generated method stub
//				if(encodedData.length>0) {
                // logger1.info("=================== get data ==================================");
                long timeStamp = System.currentTimeMillis();

                Frame frame = new Frame(si.getSourceId(), frameNr++, Frame.X264_IMAGE, encodedData, timeStamp,
                        new Rectangle(0, 0, si.getFrameWidth(), si.getFrameHeight()));

                mEncodedFrameQueue.enqueue(frame);
                // logger1.info("=================== encode end ==================================");
//				}

            }

            @Override
            public void onEncoderClosed() {
                // TODO Auto-generated method stub

            }

            @Override
            public Mat getDecodedData() {
                // TODO Auto-generated method stub
                return null;
            }
        });
        encoderWorker = encoderWorker.setCapture();

        if (Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
            logger.info("Register encoder for " + encoderWorker.getEncoderId() + " failed!");
            return null;
        }
        

        mCodecManager.startEncode(encoderId);
        return encodeFrameQueueId;
    }

    public static boolean registerEncoder(SourceInfo si, final String sourceQueueId, final String encodedQueueId) {
        String sourceId = si.getSourceId();
        final CodecType type = si.getCodecType();
        final int frameWidth = si.getFrameWidth();
        final int frameHeight = si.getFrameHeight();

        if (mMatQueueManager.getQueueById(sourceQueueId) == null) {
            logger.error("Mat queue for source " + sourceId + " hasn't register yet!");
            return false;
        }

        if (mFrameQueueManager.getQueueById(encodedQueueId) == null) {
            logger.error("Frame queue for source result " + sourceId + " hasn't register yet!");
            return false;
        }

        final String encoderId = encodedQueueId;
        si.setSourceId(encoderId);
        
        EncoderWorker encoderWorker = new EncoderWorker(si, new EncoderCallback() {
            private final Logger logger1 = LoggerFactory.getLogger(EncoderCallback.class);

            @Override
            public Mat getDecodedData() {
                // TODO Auto-generated method stub
                Mat decodedMat = null;
//				logger1.info("Before get element "+ sourceQueueId +" queue size:" + mMatQueueManager.getQueueById(sourceQueueId).getSize());
                while ((decodedMat = mMatQueueManager.getElement(sourceQueueId)) == null) {
                }
//				logger1.info("After get element "+ sourceQueueId +" queue size:" + mMatQueueManager.getQueueById(sourceQueueId).getSize());
                return decodedMat;
            }

            @Override
            public Mat beforeDataEncoded(Mat frame) {
                // TODO Auto-generated method stub
                Mat yuvMat = new Mat();
                if (frame.channels() < 3) {
                    Imgproc.cvtColor(frame, frame, Imgproc.COLOR_GRAY2BGR);
                }
                Imgproc.cvtColor(frame, yuvMat, Imgproc.COLOR_BGR2YUV_I420);
                return yuvMat;
            }

            @Override
            public void onDataEncoded(byte[] encodedData) {
                // TODO Auto-generated method stub
//				logger1.info("Before put element "+ encodedQueueId +" queue size: "+mFrameQueueManager.getQueueById(encodedQueueId).getSize());
                mFrameQueueManager.putElement(encodedQueueId, new Frame(encodedData));
//				logger1.info("After put element "+ encodedQueueId +" queue size: "+mFrameQueueManager.getQueueById(encodedQueueId).getSize());
            }

            @Override
            public void onEncoderClosed() {
                // TODO Auto-generated method stub
            }
        });
        


        if (Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
            logger.info("Register encoder for " + encoderWorker.getEncoderId() + " failed!");
            return false;
        }

        mCodecManager.startEncode(encoderId);

        logger.info("EncodeWoker has started: " + si);

        return true;
    }

    public static boolean registerDecoder(SourceInfo si, final String sourceQueueId, final String decoderQueueId) {

        if (mBufferQueueManager.getBufferQueue(sourceQueueId) == null) {
            logger.error("Buffer queue for source " + sourceQueueId + " hasn't register yet!");
            return false;
        }

        if (mMatQueueManager.getQueueById(decoderQueueId) == null) {
            logger.error("Mat queued for decoder " + decoderQueueId + " hasn't register yet!");
            return false;
        }

        final String decoderId = decoderQueueId;
        si.setSourceId(decoderId);
        
        DecoderWorker decoderWorker = new DecoderWorker(si, new DecoderCallback() {
            private int frNum = 0;
            private final Logger logger1 = LoggerFactory.getLogger(DecoderCallback.class);

            @Override
            public byte[] getEncodedData() {
                // TODO Auto-generated method stub
                byte[] encodedData = null;
//                logger1.info("start to get encoded data!");
//				do {
                encodedData = mBufferQueueManager.getBuffer(sourceQueueId, ENCODE_DATA_SEG, false);
//				} while (encodedData == null || encodedData.length < ENCODE_DATA_SEG);
                if (encodedData != null) {
//                     logger1.info("data from sourceQueue "+ sourceQueueId +" size: "+encodedData.length);
                } else {
//                     logger1.info("sourceQueue "+ sourceQueueId +" is empty!");
                }

                return encodedData;
            }

            @Override
            public void onDataDecoded(Mat frame, int[] results) {
                // TODO Auto-generated method stub
                int dataUsed = results[1];
                mBufferQueueManager.moveHeadPtr(sourceQueueId, dataUsed);

                int frameNumDecoded = results[0];
                if (frameNumDecoded > 0) {
                    // logger1.info("===================== get decoded data =================");
                    Mat rgbFrame = new Mat();
                    Imgproc.cvtColor(frame, rgbFrame, Imgproc.COLOR_YUV2BGR_I420);
                    
                     Highgui.imwrite("/home/jliu/Pictures/codec/"+frNum++ +"_codec.jpg", rgbFrame);
                    
                    mMatQueueManager.putElement(decoderQueueId, rgbFrame);
                    // logger1.info("========================== decode done ================================");
                }

            }
        });

        if (Common.CODEC_OK != mCodecManager.registerDecoder(decoderWorker)) {
            logger.error("Register decoder for " + decoderWorker.getDecoderId() + " failed!");
            return false;
        }

        mCodecManager.startDecode(decoderId);

        logger.info("DecodeWoker has started: " + decoderId);

        return true;
    }

}
