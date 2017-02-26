package edu.fudan.lwang.codec;

import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatQueueManager extends QueueManager<Mat> {
    private static Logger logger = LoggerFactory.getLogger(MatQueueManager.class);

    private static MatQueueManager instance = null;

    private MatQueueManager() {
        super();
    }

    public static synchronized MatQueueManager getInstance() {
        if (null == instance) {
            instance = new MatQueueManager();
        }
        return instance;
    }

}
