package nl.tno.stormcv.capture;

import nl.tno.stormcv.spout.CVParticleSpout;

import org.opencv.core.CvException;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author lwang
 *
 */
public class CaptureWorker extends Thread {

	private VideoCapture capture;
	private MatPackQueue mMatPackQueue;
	private long currentFrameIndex = 0;
	private Logger logger = LoggerFactory.getLogger(CVParticleSpout.class);
	
	public CaptureWorker() {
		capture = new VideoCapture("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
		capture.open("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
		mMatPackQueue = MatPackQueue.getInstance();
	}
	
	@Override
	public void run() {
		
		long start = -1;
		
		while(true) {
			try {
				if (capture.isOpened()) {
					Mat image = new Mat();
	                capture.read(image);
//	                capture.get(Highgui.CV_CAP_PROP_AUTOGRAB);
	                if (image != null) {
//		               	 System.out.println(image.dims()+"dim:" + image.width() + "x" + image.height() + "Frame: " + currentFrameIndex);
//	                	 System.out.println("read frame: " + currentFrameIndex);
	                	 while(!mMatPackQueue.push(image, false)) {
	                		 ;
	                	 }
		               	 
	                }
	                if(currentFrameIndex % 500 == 0) {
	                	start = System.currentTimeMillis();
	                }
	                currentFrameIndex++;
	                if(currentFrameIndex % 500 == 0) {
	                	long end = System.currentTimeMillis();
	                	System.out.println("rate : " + (500/((end-start)/1000.0f)));
//	                	logger.info("rate : " + (200/((end-start)/1000.0f)));
	                }
				}
			} catch(Exception e) {
                System.out.println("Null Pointer Exception during Read");
                if (e instanceof CvException) {
					capture.open("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
				}
			}
		}
		
	}
	
}
