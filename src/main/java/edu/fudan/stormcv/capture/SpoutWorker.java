package edu.fudan.stormcv.capture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpoutWorker extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(SpoutWorker.class);
    private BufferedImagePackQueue mBufferedImagePackQueue;
    private int currentSeq;

    private long start;

    public SpoutWorker() {
        mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
        currentSeq = 0;
    }

    @Override
    public void run() {
        while (true) {
            BufferedImagePackQueue.BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(currentSeq);
            if (mBufferedImagePack == null) {
                continue;
            }
            if (currentSeq == 0) {
                start = System.currentTimeMillis();
            }
            if (mBufferedImagePack.isLastPack()) {
                long end = System.currentTimeMillis();
                logger.info("spout time:" + (end - start));
                break;
            }
            currentSeq++;
            logger.info("pop image:" + mBufferedImagePack.getSeq());
        }
    }
}
