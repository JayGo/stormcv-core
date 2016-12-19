package nl.tno.stormcv.capture;

import nl.tno.stormcv.capture.BufferedImagePackQueue.BufferedImagePack;


public class SpoutWorker extends Thread {
	
	private BufferedImagePackQueue mBufferedImagePackQueue;
	private int currentSeq;
	
	private long start;
	
	public SpoutWorker() {
		mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
		currentSeq = 0;
	}
	
	@Override
	public void run() {	
		while(true) {
			BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(currentSeq);
			if(mBufferedImagePack == null) {
				continue;
			}
			if(currentSeq == 0) {
				start = System.currentTimeMillis();
			}
			if(mBufferedImagePack.isLastPack()) {
				long end = System.currentTimeMillis();
				System.out.println("spout time:" + (end - start));
				break;
			}
			currentSeq++;
			System.out.println("pop image:" + mBufferedImagePack.getSeq());
		}
	}
}
