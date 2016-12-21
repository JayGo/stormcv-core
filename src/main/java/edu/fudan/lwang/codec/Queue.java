package edu.fudan.lwang.codec;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

public class Queue<E> {
	private static Logger logger = Logger.getLogger(Queue.class);
	public  static final int DEFAUTL_MAT_LIMIT = 150;
	public static final int DEFAULT_FRAME_LIMIT = 1600;
	private int limit;
	private int size = 0;
	private String queueId;

	private int dropCount = 0;

	private BlockingQueue<E> queue = new LinkedBlockingDeque<E>();

	public Queue(String quueId) {
		this(quueId, DEFAUTL_MAT_LIMIT);
	}

	public Queue(String queueId, int limit) {
		this.queueId = queueId;
		this.limit = limit;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getSize() {
		return size;
	}

	public String getQueueId() {
		return queueId;
	}

	public void setQueueId(String queueId) {
		this.queueId = queueId;
	}

	public void enqueue(E input) {
		// logger.info("Queue size: "+size + ", BlockingQueue size: "+queue.size());
		if (size == limit) {
			try {
				if (queue.take() != null) {
					logger.info("drop count: " + ++dropCount);
					--size;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (queue.offer(input)) {
			++size;
		}
	}

	public E dequeue() {
		E result = null;
		result = queue.poll();
		
		if (result != null) {
			--size;
			dropCount=0;
		} else {
			// logger.info("The queue has no element!");
		}
		return result;
	}
	
	public void clearAll() {
		queue.clear();
	}
}
