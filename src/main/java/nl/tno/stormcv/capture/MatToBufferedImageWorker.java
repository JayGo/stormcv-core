package nl.tno.stormcv.capture;

import java.awt.image.BufferedImage;

import nl.tno.stormcv.capture.MatPackQueue.MatPack;
import nl.tno.stormcv.util.ImageUtils;


public class MatToBufferedImageWorker extends Thread {

	private MatPackQueue mMatPackQueue;
	private BufferedImagePackQueue mBufferedImagePackQueue;
	
	public MatToBufferedImageWorker() {
		mMatPackQueue = MatPackQueue.getInstance();
		mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
	}
	
	@Override
	public void run() {
		while (true) {
			MatPack mMatPack = mMatPackQueue.pop();
			if(mMatPack == null) {
				continue;
			}
//			System.out.println("Transform Thread" + Thread.currentThread().getId() + "is solving Frame " + mMatPack.getSeq());
			if(mMatPack.isLastPack()) {
				BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
				while(!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
					;
				}
				System.out.println("Transform Thread" + Thread.currentThread().getId() + "is exiting");
				break;
			}
			BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
			while(!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
				;
			}
		}
	}
	
}
