package edu.fudan.lwang.codec;

import org.apache.log4j.Logger;
import org.opencv.core.Mat;

public class MatQueueManager extends QueueManager<Mat>{
	private static  Logger logger = Logger.getLogger(MatQueueManager.class);
	
	private static MatQueueManager instance = null;
	
	private MatQueueManager() {
		super();
	}

	public static synchronized MatQueueManager getInstance() {
		if(null == instance) {
			instance = new MatQueueManager();
		}
		return instance;
	}
	
}
