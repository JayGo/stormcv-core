package edu.fudan.lwang.codec;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;



public class BufferQueueManager {
	
    private final static Logger logger = Logger.getLogger(BufferQueueManager.class);
    
	private Map<String,BufferQueue> bufferQueues;
	
	private static BufferQueueManager mBufferQueueManager;
	
	private BufferQueueManager() {
		bufferQueues = new WeakHashMap<String, BufferQueue>();
	}
	
	public synchronized static BufferQueueManager getInstance() {
		if(mBufferQueueManager == null) {
			mBufferQueueManager = new BufferQueueManager();
		}
		return mBufferQueueManager;
	}
	
	/**
	 * Description :
	 * 		register a buffer queue
	 * @param queueId
	 * @param maxQueueSize, better to be 2^N - 1 for memory align purpose
	 */
	public void registerBufferQueue(String queueId, int maxQueueSize) {
		BufferQueue mBufferQueue = new BufferQueue(queueId, maxQueueSize);
		bufferQueues.put(queueId, mBufferQueue);
		logger.info("Register BuffferQueue: "+queueId);
	}
	
	public void registerBufferQueue(String queueId) {
		BufferQueue mBufferQueue = new BufferQueue(queueId);
	}
	
	/**
	 * Description :
	 * 		get certain queue size by queue id
	 * @param queueId
	 * @return
	 */
	public int getQueueSize(String queueId) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		return mBufferQueue.getQueueSize();
	}
	

	
	public BufferQueue getBufferQueue(String queueId) {
		return bufferQueues.get(queueId);
	}
	
	/**
	 * Description :
	 * 		get buffer with certain length and queue_id
	 * @param queueId
	 * @param length
	 * @return
	 */
	public byte[] getBuffer(String queueId, int length) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		return mBufferQueue.getBuffer(length);
	}
	
	/**
	 * Description :
	 * 		get buffer with certain length and queue_id
	 * @param queueId
	 * @param length
	 * @param isMoveIndex, weather move queue head index or not
	 * @return
	 */
	public byte[] getBuffer(String queueId, int length, boolean isMoveIndex) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		return mBufferQueue.getBuffer(length, isMoveIndex);
	}
	
	/**
	 * Description :
	 * 		push buffer into certain queue
	 * @param queueId
	 * @param buffer
	 * @return succeed or not 
	 */
	public boolean fillBuffer(String queueId, byte[] buffer) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		return mBufferQueue.fillBuffer(buffer);
	}
	
	/**
	 * Description :
	 * 		push buffer into certain queue
	 * @param queueId
	 * @param buffer
	 * @param isMoveIndex, weather move queue tail index or not
	 * @return
	 */
	public boolean fillBuffer(String queueId, byte[] buffer, boolean isMoveIndex) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		return mBufferQueue.fillBuffer(buffer, isMoveIndex);
	}
	
	/**
	 * Description :
	 * 		release a buffer queue
	 * @param queueId
	 */
	public void releaseBufferQueue(String queueId) {
		bufferQueues.remove(queueId);
	}
	
	/**
	 * Description :
	 * 		Move head Pointer with length 'offset'
	 * @param offset
	 */
	public void moveHeadPtr(String queueId, int offset) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		mBufferQueue.moveHeadPtr(offset);
	}
	
	/**
  	 * Description :
  	 *		Move tail Pointer with length 'offset'
	 * @param queueId
	 * @param offset
	 */
	public void moveTailPtr(String queueId, int offset) {
		BufferQueue mBufferQueue = bufferQueues.get(queueId);
		mBufferQueue.moveTailPtr(offset);
	}
}