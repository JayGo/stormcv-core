package edu.fudan.stormcv.bolt;

import edu.fudan.lwang.codec.*;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.operation.single.ISingleInputOperation;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * A basic {@link CVParticleBolt} implementation that works with single items received (hence maintains no
 * history of items received). The bolt will ask the provided {@link ISingleInputOperation} implementation to
 * work on the input it received and produce zero or more results which will be emitted by the bolt.
 * If an operation throws an exception the input will be failed in all other situations the input will be acked.
 *
 * @author Corne Versloot
 */
public class SingleH264InputBolt extends CVParticleBolt {

    /**
     *
     */
    private static final long serialVersionUID = 2919345121819900242L;

    private final Logger logger = LoggerFactory.getLogger(SingleH264InputBolt.class);

    private MatQueueManager mMatQueueManager;
    private BufferQueueManager mBufferQueueManager;
    private FrameQueueManager mFrameQueueManager;

    private String mDecodeQueueId;
    private String mSourceQueueId;
    private Queue<Mat> mDecodedMatQueue;
    private BufferQueue mSourceBufferQueue;
    private String mResultMatQueueId;
    private Queue<Mat> mResultMatQueue;

    private SourceInfo mSourceInfo;

    private ISingleInputOperation<? extends CVParticle> operation;
    private long startTime;
    private long endTime;
    private int count;


    /**
     * Constructs a SingleInputOperation
     *
     * @param operation the operation to be performed
     */
    public SingleH264InputBolt(ISingleInputOperation<? extends CVParticle> operation) {
        this.operation = operation;
    }

    public SingleH264InputBolt setSourceInfo(SourceInfo sInfo) {
        this.mSourceInfo = sInfo;
        return this;
    }

    private boolean setDecoder() {
        if (mSourceInfo == null) {
            logger.info("setCodecer: the SourceInfo is null");
            return false;
        }

        if (operation == null) {
            logger.info("setCodecer: the operation is null");
            return false;
        }

        if (!Codec.registerDecoder(mSourceInfo, mSourceQueueId, mDecodeQueueId)) {
            logger.error("Register decoder for source: " + mSourceQueueId + " failed!");
            return false;
        }

        return true;
    }


    private String mEncodedQueueId;
    private Queue<Frame> mEncodedFrameQueue;

    private void init() {
        mDecodeQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() + "_decode";
        mSourceQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() + "_source";
        mResultMatQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() + "_result";
        mEncodedQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() + "_encode";


        // Register the source queue.
        mBufferQueueManager = BufferQueueManager.getInstance();
        mBufferQueueManager.registerBufferQueue(mSourceQueueId);
        mSourceBufferQueue = mBufferQueueManager.getBufferQueue(mSourceQueueId);


        // Register the mat buffer queue for decoder.
        mMatQueueManager = MatQueueManager.getInstance();
        mMatQueueManager.registerQueue(mDecodeQueueId);

        // Reigster the mat queue for processing result
        mMatQueueManager.registerQueue(mResultMatQueueId);

        // Start to decode the data sourcing from mSourceQueueId
        setDecoder();
        mDecodedMatQueue = mMatQueueManager.getQueueById(mDecodeQueueId);
        mResultMatQueue = mMatQueueManager.getQueueById(mResultMatQueueId);

        // Register the result encoded queue
        mFrameQueueManager = FrameQueueManager.getInstance();
        mFrameQueueManager.registerQueue(mEncodedQueueId);
        mEncodedFrameQueue = mFrameQueueManager.getQueueById(mEncodedQueueId);

        if (setEncoder()) {
            mEncoderWorker = CodecManager.getInstance().getEncoder(mEncodedQueueId);
        }
    }

    private EncoderWorker mEncoderWorker;

    private boolean setEncoder() {

        if (!Codec.registerEncoder(mSourceInfo, mResultMatQueueId, mEncodedQueueId)) {
            logger.error("Register encoder for result: " + mSourceQueueId + " failed!");
            return false;
        }

        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    void prepare(Map stormConf, TopologyContext context) {
        try {
            LibLoader.loadOpenCVLib();
            LibLoader.loadH264CodecLib();
            LibLoader.loadHgCodecLib();
            operation.prepare(stormConf, context);
            init();
        } catch (Exception e) {
            logger.error("Unale to prepare Operation ", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(operation.getSerializer().getFields());
    }

    @Override
    List<? extends CVParticle> execute(CVParticle input) throws Exception {
        if (this.count == 0) {
            this.startTime = System.currentTimeMillis();
        }
        // fill buffer with input.imageBytes first.
        List<? extends CVParticle> results = operation.execute(input, new OperationHandler<Mat>() {
            Logger logger1 = LoggerFactory.getLogger(OperationHandler.class);

            @Override
            public boolean fillSourceBufferQueue(Frame frame) {
                // TODO Auto-generated method stub
                byte[] encodedData = frame.getImageBytes();
                // logger1.info("get image bytes length: "+encodedData.length);
                while (!mSourceBufferQueue.fillBuffer(encodedData)) {
                    logger1.info("atempt to fill buffer...");
                }
                return true;
            }

            @Override
            public Mat getDecodedData() {
                // TODO Auto-generated method stub
                return mDecodedMatQueue.dequeue();
            }

            @Override
            public byte[] getEncodedData(Mat proccessedResult, String imageType) {
                // TODO Auto-generated method stub
                mResultMatQueue.enqueue(proccessedResult);

//				logger1.info("Before dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
                Frame encodedFrame = null;
                while ((encodedFrame = mEncodedFrameQueue.dequeue()) == null) {
                    // logger1.info("encoded frame queue has no element!");
                }
                ;
//				 logger1.info("After dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
//				if((encodedFrame = mEncodedFrameQueue.dequeue())==null) {
//					return null;
//				} else {
//					return encodedFrame.getImageBytes();
//				}
                return encodedFrame.getImageBytes();
            }
        });

        this.count++;
        if (this.count == 500) {
            endTime = System.currentTimeMillis();
            logger.info("{} Rate: {}", operation.getContext(), (500 / ((endTime - startTime) / 1000.0f)));
            this.count = 0;
        }

        // copy metadata from input to output if configured to do so
//        for (CVParticle s : results) {
//            for (String key : input.getMetadata().keySet()) {
//                if (!s.getMetadata().containsKey(key)) {
//                    s.getMetadata().put(key, input.getMetadata().get(key));
//                }
//            }
//        }
        return results;
    }
}
