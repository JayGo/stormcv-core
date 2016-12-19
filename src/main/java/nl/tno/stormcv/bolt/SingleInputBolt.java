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

import edu.fudan.lwang.codec.BufferQueue;
import edu.fudan.lwang.codec.BufferQueueManager;
import edu.fudan.lwang.codec.Codec;
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
	

	private MatQueueManager mMatQueueManager;
	private BufferQueueManager mBufferQueueManager;
	
	private String mDecodeQueueId;
	private String mSourceQueueId;	
	private Queue<Mat> mDecodedMatQueue;
	private BufferQueue mSourceBufferQueue;
	
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
	

	private void init() {
		mDecodeQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_decode";
		mSourceQueueId = mSourceInfo.getEncodeQueueId() + "_" + operation.getContext() +"_source";
		
		// Register the source queue.
		mBufferQueueManager = BufferQueueManager.getInstance();
		mBufferQueueManager.registerBufferQueue(mSourceQueueId);
		
		// Register the mat buffer queue for decoder.
		mMatQueueManager = MatQueueManager.getInstance();
		mMatQueueManager.registerQueue(mDecodeQueueId);
		
		// Start to decode the data sourcing from mSourceQueueId
		setDecoder();
		
		mSourceBufferQueue = mBufferQueueManager.getBufferQueue(mSourceQueueId);
		mDecodedMatQueue = mMatQueueManager.getQueueById(mDecodeQueueId);
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
	List<? extends CVParticle> execute(CVParticle input) throws Exception{
		
		List<? extends CVParticle> results = operation.execute(input, new OperationHandler() {

			@Override
			public boolean fillSourceBufferQueue(Frame frame) {
				// TODO Auto-generated method stub
				byte [] encodedData = frame.getImageBytes();
				while(mSourceBufferQueue.fillBuffer(encodedData));
				return false;
			}

			@Override
			public Mat getMat() {
				// TODO Auto-generated method stub
				return mDecodedMatQueue.dequeue();
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
