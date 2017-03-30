package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.stormcv.model.Frame;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static SourceInfo fetchSourceInfo(String videoAddr, String streamId, CodecType type) {
        SourceInfo sourceInfo = null;
        VideoCapture videoCapture = new VideoCapture();
        if (!videoCapture.open(videoAddr)) {
            logger.info("VideoCapture open " + videoAddr + " failed!");
            return sourceInfo;
        }
        sourceInfo = new SourceInfo();
        sourceInfo.setFrameWidth((int) videoCapture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH));
        sourceInfo.setFrameHeight((int) videoCapture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT));
        sourceInfo.setEncodeQueueId(streamId);
        sourceInfo.setType(type);
        sourceInfo.setVideoAddr(videoAddr);
        return sourceInfo;
    }

    public static String startEncodeToBuffer(SourceInfo si) {
        String encoderId = null;
        String videoAddr = si.getVideoAddr();
        String streamId = si.getEncodeQueueId();
        CodecType type = si.getType();
        final VideoCapture capture = new VideoCapture();

        if (!capture.open(videoAddr)) {
            logger.info("Video open failed!");
            return null;
        }

        encoderId = streamId;
        final String encodeQueueId = encoderId;
        mBufferQueueManager.registerBufferQueue(encodeQueueId, maxEncoderQueueSize);

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
                while (!mBufferQueueManager.fillBuffer(encodeQueueId, encodedData)) ;
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
        return encodeQueueId;
    }

    public static String startEncodeToFrameQueue(final SourceInfo si) {
        String encoderId = null;
        String videoAddr = si.getVideoAddr();
        String streamId = si.getEncodeQueueId();
        CodecType type = si.getType();
        final VideoCapture capture = new VideoCapture();

        if (!capture.open(videoAddr)) {
            logger.info("Video open failed!");
            return null;
        }

        encoderId = streamId;
        final String encodeFrameQueueId = encoderId;

        mFrameQueueManager.registerQueue(encodeFrameQueueId, maxEncoderQueueFrames);
        if ((mEncodedFrameQueue = mFrameQueueManager.getQueueById(encodeFrameQueueId)) == null) {
            logger.info("Register ecodedFrameQueue for " + encodeFrameQueueId + " failed!");
            return null;
        }

        EncoderWorker encoderWorker = EncoderFactory.create(type).build(videoAddr, encoderId, capture, new EncoderCallback() {
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

                Frame frame = new Frame(si.getEncodeQueueId(), frameNr++, Frame.X264_IMAGE, encodedData, timeStamp,
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

        if (null == encoderWorker) {
            logger.info("Unknow encoder type, encoderWorker is null!");
            return null;
        }

        if (Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
            logger.info("Register encoder for " + encoderWorker.getEncoderId() + " failed!");
            return null;
        }

        mCodecManager.startEncode(encoderId);
        return encodeFrameQueueId;
    }

    public static boolean registerEncoder(SourceInfo si, final String sourceQueueId, final String encodedQueueId) {
        String sourceId = si.getEncodeQueueId();
        final CodecType type = si.getType();
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

        EncoderWorker encoderWorker = EncoderFactory.create(type).build(encoderId, frameWidth, frameHeight,

                new EncoderCallback() {
                    private final Logger logger1 = LoggerFactory.getLogger(EncoderCallback.class);

                    @Override
                    public Mat getDecodedData() {
                        // TODO Auto-generated method stub
                        Mat decodedMat = null;
//						logger1.info("Before get element "+ sourceQueueId +" queue size:" + mMatQueueManager.getQueueById(sourceQueueId).getSize());
                        while ((decodedMat = mMatQueueManager.getElement(sourceQueueId)) == null) {
                        }
//						logger1.info("After get element "+ sourceQueueId +" queue size:" + mMatQueueManager.getQueueById(sourceQueueId).getSize());
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
//						logger1.info("Before put element "+ encodedQueueId +" queue size: "+mFrameQueueManager.getQueueById(encodedQueueId).getSize());
                        mFrameQueueManager.putElement(encodedQueueId, new Frame(encodedData));
//						logger1.info("After put element "+ encodedQueueId +" queue size: "+mFrameQueueManager.getQueueById(encodedQueueId).getSize());
                    }

                    @Override
                    public void onEncoderClosed() {
                        // TODO Auto-generated method stub
                    }
                });

        if (null == encoderWorker) {
            logger.info("Unknow encoder type, encoderWorker is null!");
            System.exit(1);
            return false;
        }

        if (Common.CODEC_OK != mCodecManager.registerEncoder(encoderWorker)) {
            logger.info("Register encoder for " + encoderWorker.getEncoderId() + " failed!");
            return false;
        }

        mCodecManager.startEncode(encoderId);

        logger.info("EncodeWoker has started: " + si);

        return true;
    }

    public static boolean registerDecoder(SourceInfo si, final String sourceQueueId, final String decoderQueueId) {

        final CodecType type = si.getType();
        final int frameWidth = si.getFrameWidth();
        final int frameHeight = si.getFrameHeight();

        if (mBufferQueueManager.getBufferQueue(sourceQueueId) == null) {
            logger.error("Buffer queue for source " + sourceQueueId + " hasn't register yet!");
            return false;
        }

        if (mMatQueueManager.getQueueById(decoderQueueId) == null) {
            logger.error("Mat queued for decoder " + decoderQueueId + " hasn't register yet!");
            return false;
        }

        final String decoderId = decoderQueueId;

        DecoderWorker decoderWorker = DecoderFactory.create(type).build(decoderId, frameWidth, frameHeight,
                new DecoderCallback() {
                    private int frNum = 0;
                    private final Logger logger1 = LoggerFactory.getLogger(DecoderCallback.class);

                    @Override
                    public byte[] getEncodedData() {
                        // TODO Auto-generated method stub
                        byte[] encodedData = null;
//						do {
                        encodedData = mBufferQueueManager.getBuffer(sourceQueueId, ENCODE_DATA_SEG, false);
//						} while (encodedData == null || encodedData.length < ENCODE_DATA_SEG);
                        if (encodedData != null) {
                            // logger1.info("data from sourceQueue "+ sourceQueueId +" size: "+encodedData.length);
                        } else {
                            // logger1.info("sourceQueue "+ sourceQueueId +" is empty!");
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
                            
//                            Highgui.imwrite("/home/jliu/Pictures/codec/"+frNum++ +"_codec.jpg", rgbFrame);
                            
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
