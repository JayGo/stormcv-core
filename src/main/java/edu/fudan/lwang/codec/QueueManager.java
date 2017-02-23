package edu.fudan.lwang.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class QueueManager<E> {
	private Map<String,Queue<E>> map;
	private static Logger logger = LoggerFactory.getLogger(QueueManager.class);
	
	protected QueueManager() {
		map = new HashMap<String, Queue<E>>();
	}
//	private static MatQueueManager instance = null;
	
//	private MatQueueManager() {
//		map = new HashMap<String,MatQueue>();
//	}
//	public static synchronized MatQueueManager getInstance() {
//		if(null == instance) {
//			instance = new MatQueueManager();
//			
//		}
//		return instance;
//	}
	
	public synchronized void registerQueue(String queueId) {
		Queue<E> q = new Queue<E>(queueId);
		map.put(queueId, q);
		logger.info("Register MatQueue: "+queueId);
	}
	
	public synchronized void registerQueue(String queueId, int limit) {
		Queue<E> q = new Queue<E>(queueId, limit);
		map.put(queueId, q);
		logger.info("Register MatQueue: "+queueId);
	}
	
	public Queue<E> getQueueById(String queueId) {
		return map.get(queueId);
	}
	
	public E getElement(String queueId) {
		Queue<E> q = getQueueById(queueId);
		if(null == q) {
			logger.info("getElement: "+queueId+"'s Queue is not exsited!");
			return null;
		}
		return q.dequeue();
	}
	
	public void putElement(String queueId, E input) {
		Queue<E> q = getQueueById(queueId);
		if(null == q) {
			logger.error("getMat: "+queueId+"'s Queue is not exsited!");
			return;
		}
		q.enqueue(input);
	}
	
	public synchronized void unRegisterMatQueue(String queueId) {
		Queue<E> q = map.get(queueId);
		if(null == q) {
			logger.info("unRegister: "+queueId+"'s Queue is not exsited!");
			return;
		}
		map.remove(queueId);
		q = null;
	}
}
