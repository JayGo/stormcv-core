package edu.fudan.lwang.codec;


import nl.tno.stormcv.model.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrameQueueManager extends QueueManager<Frame>{
	private static Logger logger = LoggerFactory.getLogger(FrameQueueManager.class);
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
