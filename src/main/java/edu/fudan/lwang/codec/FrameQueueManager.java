package edu.fudan.lwang.codec;

import org.apache.log4j.Logger;

import nl.tno.stormcv.model.Frame;

public class FrameQueueManager extends QueueManager<Frame>{
	private static  Logger logger = Logger.getLogger(FrameQueueManager.class);
	private static FrameQueueManager instance = null;
	
	private FrameQueueManager() {
		super();
	}

	public static synchronized FrameQueueManager getInstance() {
		if(null == instance) {
			instance = new FrameQueueManager();
		}
		return instance;
	}
}
