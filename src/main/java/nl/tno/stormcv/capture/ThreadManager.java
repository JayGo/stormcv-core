package nl.tno.stormcv.capture;

import java.util.ArrayList;
import java.util.List;

public class ThreadManager {

	private static ThreadManager mThreadManager;
	private List<MatToBufferedImageWorker> mMatToBufferedImageWorkers;
	private SpoutWorker mSpoutWorker;
	private CaptureWorker mCaptureWorker;

	private final int mMatToBufferedImageWorkerSize = 4;

	private ThreadManager() {
		mMatToBufferedImageWorkers = new ArrayList<MatToBufferedImageWorker>();
		mSpoutWorker = new SpoutWorker();
		mCaptureWorker = new CaptureWorker();
	}

	public static synchronized ThreadManager getInstance() {
		if (mThreadManager == null) {
			mThreadManager = new ThreadManager();
		}
		return mThreadManager;
	}

	public void startProcessing() {
		for (int i = 0; i < mMatToBufferedImageWorkerSize; i++) {
			MatToBufferedImageWorker mMatToBufferedImageWorker = new MatToBufferedImageWorker();
			mMatToBufferedImageWorkers.add(mMatToBufferedImageWorker);
			mMatToBufferedImageWorker.start();
		}
	}
	
	public void startProcessingAndSpouting() {
		startProcessing();
		mSpoutWorker.start();
	}

	public void synThreads() {
		try {
			for (int i = 0; i < mMatToBufferedImageWorkerSize; i++) {
				MatToBufferedImageWorker mMatToBufferedImageWorker = mMatToBufferedImageWorkers
						.get(i);
				mMatToBufferedImageWorker.join();
			}
			mSpoutWorker.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public void endProcessingAndSpouting() {
		try {
			mSpoutWorker.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < mMatToBufferedImageWorkerSize; i++) {
			MatToBufferedImageWorker mMatToBufferedImageWorker = mMatToBufferedImageWorkers
					.get(i);
			mMatToBufferedImageWorker.stop();
		}
	}
	
	public void startProcessingAndReading() {
		mCaptureWorker.start();
		startProcessing();
	}
}
