package edu.fudan.lwang.codec;

public class BufferQueue {
	
	public final static int DEFAULT_MAX_SIZE = 32*1024*1024 - 1;
	private String queueId;
	private byte[] buffer;
	private int head;
	private int tail;
	private int maxQueueSize;
	
	public BufferQueue(String queueId) {
		this(queueId, DEFAULT_MAX_SIZE);
	}
	
	/**
	 * better set maxQueueSize equals to 2^N - 1
	 * @param queueId
	 * @param maxQueueSize
	 */
	public BufferQueue(String queueId, int maxQueueSize) {
		this.queueId = queueId;
		this.maxQueueSize = maxQueueSize + 1;
		buffer = new byte[this.maxQueueSize];
		head = tail = 0;
	}
	
	public String getQueueId() {
		return queueId;
	}
	
	public int getQueueSize() {
		if(tail >= head) {
			return tail - head;
		}
		return (tail - head) + maxQueueSize;
	}
	
	public synchronized byte[] getBuffer(int length, boolean isMoveIndex) {
		
		int currentLength = getQueueSize();
		if(length > currentLength) {
			//log here
			return null;
		}
		
		byte[] bufferNeeded = new byte[length];
		if(tail >= head) {
			System.arraycopy(buffer, head, bufferNeeded, 0, length);
		}
		else {
			int firstSegLength = maxQueueSize - head;
			if(firstSegLength >= length) {
				System.arraycopy(buffer, head, bufferNeeded, 0, length);
			}
			else {
				int secondSegLength = length - firstSegLength;
				System.arraycopy(buffer, head, bufferNeeded, 0, firstSegLength);
				System.arraycopy(buffer, 0, bufferNeeded, firstSegLength, secondSegLength);
			}
		}
		
		if(isMoveIndex) {
			head = (head + length) % maxQueueSize;
		}
		
		return bufferNeeded;
	}
	
	public synchronized byte[] getBuffer(int length) {
		return getBuffer(length, true);
	}
	
	public synchronized boolean fillBuffer(byte[] bufferInput, boolean isMoveIndex) {
		
		int currentLength = getQueueSize();
		int bufferLength = bufferInput.length;
		
		if(bufferLength + currentLength >= maxQueueSize) {
			//log here
			return false;
		}
		
		
		if(tail < head) {
			System.arraycopy(bufferInput, 0, buffer, tail, bufferLength);
		}
		else {
			int firstSegLength = maxQueueSize - tail;
			if(firstSegLength >= bufferLength) {
				System.arraycopy(bufferInput, 0, buffer, tail, bufferLength);
			}
			else {
				int secondSegLength = bufferLength - firstSegLength;
				System.arraycopy(bufferInput, 0, buffer, tail, firstSegLength);
				System.arraycopy(bufferInput, firstSegLength, buffer, 0, secondSegLength);
			}
		}
		
		if(isMoveIndex) {
			tail = (tail + bufferLength) % maxQueueSize;
		}
		
		return true;
	}
	
	public synchronized boolean fillBuffer(byte[] bufferInput) {
		return fillBuffer(bufferInput, true);
	}
	
	public synchronized void moveHeadPtr(int offset) {
		head = (head + offset) % maxQueueSize;
	}
	
	public synchronized void moveTailPtr(int offset) {
		tail = (tail + offset) % maxQueueSize;
	}
}
