package nl.tno.stormcv.bolt;

import java.nio.Buffer;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.operation.ISingleInputOperation;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.fudan.lwang.codec.BufferQueue;
import edu.fudan.lwang.codec.BufferQueueManager;
import edu.fudan.lwang.codec.Codec;
import edu.fudan.lwang.codec.CodecManager;
import edu.fudan.lwang.codec.EncoderWorker;
import edu.fudan.lwang.codec.FrameQueueManager;
import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.lwang.codec.MatQueueManager;
import edu.fudan.lwang.codec.Queue;
import edu.fudan.lwang.codec.SourceInfo;

/**
 * A basic {@link CVParticleBolt} implementation that works with single items received (hence maintains no
 * history of items received). The bolt will ask the provided {@link ISingleInputOperation} implementation to
 * work on the input it received and produce zero or more results which will be emitted by the bolt. 
 * If an operation throws an exception the input will be failed in all other situations the input will be acked.
 * 
 * @author Corne Versloot
 *
 */
public class SingleInputBolt extends CVParticleBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2919345121819900242L;
	
	private final Logger logger = LoggerFactory.getLogger(SingleInputBolt.class);
	
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


	/**
	 * Constructs a SingleInputOperation 
	 * @param operation the operation to be performed
	 */
	public SingleInputBolt(ISingleInputOperation<? extends CVParticle> operation){
		this.operation = operation;
	}
	
	public SingleInputBolt setSourceInfo(SourceInfo sInfo) {
		this.mSourceInfo = sInfo;
		return this;
	}
	
	private boolean setDecoder() {
		if(mSourceInfo == null) {
			logger.info("setCodecer: the SourceInfo is null");
			return false;
		}
		
		if(operation == null) {
			logger.info("setCodecer: the operation is null");
			return false;
		}
		
		if(!Codec.registerDecoder(mSourceInfo, mSourceQueueId, mDecodeQueueId)) {
			logger.error("Register decoder for source: "+mSourceQueueId+" failed!");
			return false;
		}
		
		return true;
	}
	

	private String mEncodedQueueId;
	private Queue<Frame> mEncodedFrameQueue;
	
	private void init() {
		mDecodeQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_decode";
		mSourceQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_source";
		mResultMatQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_result";
		mEncodedQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_encode";
		
		
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
		
		if(setEncoder()) {
			mEncoderWorker = CodecManager.getInstance().getEncoder(mEncodedQueueId);
		}
	}
	
	private EncoderWorker mEncoderWorker;
	private boolean setEncoder() {

		if(!Codec.registerEncoder(mSourceInfo, mResultMatQueueId, mEncodedQueueId)) {
			logger.error("Register encoder for result: "+mSourceQueueId+" failed!");
			return false;
		}
		
		return true;
	}
	
	/**
	 * Set decoder for this bolt. The method must be called after its constructor.
	 * @param the infomation of its video source to be decoded 
	 */
	

	@SuppressWarnings("rawtypes")
	@Override
	void prepare(Map stormConf, TopologyContext context) {
		try {
			System.load("/usr/local/opencv/share/OpenCV/java/libopencv_java2413.so");
			System.load("/usr/local/LwangCodec/lib/libHgCodec.so");
			
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
		// fill buffer with input.imageBytes first.
		List<? extends CVParticle> results = operation.execute(input, new OperationHandler() {
			
			org.apache.log4j.Logger logger1 = org.apache.log4j.Logger.getLogger(OperationHandler.class);
			
			@Override
			public boolean fillSourceBufferQueue(Frame frame) {
				// TODO Auto-generated method stub
				byte [] encodedData = frame.getImageBytes();
				// logger1.info("get image bytes length: "+encodedData.length);
				while(!mSourceBufferQueue.fillBuffer(encodedData)) {
					logger1.info("atempt to fill buffer...");
				}
				return true;
			}

			@Override
			public Mat getMat() {
				// TODO Auto-generated method stub
				return mDecodedMatQueue.dequeue();
			}

			@Override
			public byte[] getEncodedData(Mat proccessedResult) {
				// TODO Auto-generated method stub
				mResultMatQueue.enqueue(proccessedResult);
				
//				logger1.info("Before dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
				Frame encodedFrame = null;
				 while((encodedFrame = mEncodedFrameQueue.dequeue())==null){
					 // logger1.info("encoded frame queue has no element!");
				 };
//				 logger1.info("After dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
//				if((encodedFrame = mEncodedFrameQueue.dequeue())==null) {
//					return null;
//				} else {
//					return encodedFrame.getImageBytes();
//				}
				 return encodedFrame.getImageBytes();
			}
		});
		
		// copy metadata from input to output if configured to do so
		for(CVParticle s : results){
			for(String key : input.getMetadata().keySet()){
				if(!s.getMetadata().containsKey(key)){
					s.getMetadata().put(key, input.getMetadata().get(key));
				}
			}
		}
		return results;
	}
	
	

}
