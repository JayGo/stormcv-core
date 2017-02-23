package nl.tno.stormcv.util.reader;

import nl.tno.stormcv.capture.BufferedImagePackQueue;
import nl.tno.stormcv.capture.ThreadManager;
import nl.tno.stormcv.capture.BufferedImagePackQueue.BufferedImagePack;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.MatImage;

import nl.tno.stormcv.util.ImageUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class reads a video stream or file, decodes frames and puts those in a queue for further processing.
 * The StreamReader will automatically throttle itself based on the size of the queue it writes the frames to.
 * This is done to avoid memory overload if production of frames is higher than the consumption. The actual decoding of
 * frames is done by Xuggler which in turn uses FFMPEG (xuggler jar file is shipped with ffmpeg binaries).
 *
 * @author Corne Versloot
 */
public class OpenCVStreamReader2 implements Runnable {

    private Logger logger = LoggerFactory.getLogger(OpenCVStreamReader2.class);
    private String streamId;
    private int frameSkip;
    private int groupSize;
    private long frameNr; // number of the frame read so far
    private boolean running = false; // indicator if the reader is still active
    private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
    private LinkedBlockingQueue<MatImage> matQueue; // queue used to store frames
    private long lastRead = -1; // used to determine if the EOF was reached if Xuggler does not detect it
    private int sleepTime;
    private int frameMs = -1;

    private String streamLocation;
    private LinkedBlockingQueue<String> videoList = null;
    private String imageType = Frame.JPG_IMAGE;

	public OpenCVStreamReader2(LinkedBlockingQueue<String> videoList,
			String imageType, int frameSkip, int groupSize, int sleepTime,
			boolean uniqueIdPerFile, LinkedBlockingQueue<Frame> frameQueue,
			LinkedBlockingQueue<MatImage> matQueue) {
		this.videoList = videoList;
		this.imageType = imageType;
		this.frameSkip = Math.max(1, frameSkip);
		this.groupSize = Math.max(1, groupSize);
		this.sleepTime = sleepTime;
		this.frameQueue = frameQueue;
		this.matQueue = matQueue;
		this.streamId = "" + this.hashCode(); // give a default streamId
		lastRead = System.currentTimeMillis() + 10000;

	}

	public OpenCVStreamReader2(String streamId, String streamLocation,
			String imageType, int frameSkip, int groupSize, int sleepTime,
			LinkedBlockingQueue<Frame> frameQueue,
			LinkedBlockingQueue<MatImage> matQueue) {
		this.streamLocation = streamLocation;
		this.imageType = imageType;
		this.frameSkip = Math.max(1, frameSkip);
		this.groupSize = Math.max(1, groupSize);
		this.sleepTime = sleepTime;
		this.frameQueue = frameQueue;
		this.matQueue = matQueue;
		this.streamId = streamId;
		lastRead = System.currentTimeMillis() + 10000;
	}

	/*
     * Start reading the provided URL
     */
    @Override
	public void run() {
        running = true;
        while (running) {
            try {
                // if a url was provided read it
                if (videoList == null && streamLocation != null) {
                    logger.info("Start reading stream: " + streamLocation);
                    startStreamReading(streamLocation);
                    //mediaReader = ToolFactory.makeReader(streamLocation);
                } else if (videoList != null) {
                   //TODO:need to process
                }
            } catch (Exception e) {
                logger.warn("Stream closed unexpectatly: " + e.getMessage(), e);
                // sleep a minute and try to read the stream again
                Utils.sleep(1 * 10 * 1000);  // have change to 10s for test
            }
        }
        running = false;
    }


    public void startStreamReading(String location) {
    	
    	BufferedImagePackQueue mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
    	ThreadManager.getInstance().startProcessingAndReading();
    	
        while (true) {
            try {
            	
    			BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(frameNr);
    			if(mBufferedImagePack == null) {
    				continue;
    			}
    			BufferedImage bufferedImage = mBufferedImagePack.getImage();
    			
            	byte[] buffer = ImageUtils.imageToBytes(bufferedImage, imageType);
                long timestamp = System.currentTimeMillis();
                Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
                newFrame.getMetadata().put("uri", streamLocation);
                frameQueue.put(newFrame);
            } catch (Exception e) {
                e.printStackTrace();
            }
            frameNr++;
        }                
    }
    
//    public void startStreamReading(String location) {
//
//    	VideoCapture capture = new VideoCapture();
//
//        Mat mat = new Mat();
//        //double propid = 0;
//        frameNr = 0;
//        
//        capture.open(location);
//        int width = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
//        int height = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
//        double frameRate = capture.get(5);
//        //frameRate = cap.get(Highgui.CV_CAP_PROP_FPS);
//
//        logger.info(width + "x" + height + " at " + frameRate);
////        BufferedImage bufferedImage = null;
//        
//        if (capture.isOpened()) {
//            logger.info("Video is captured at " + location);
//            byte[] matbytes = null;
//            while (true) {
//                try {
//                    //capture.retrieve(image); //same results as .read()
//                    capture.read(mat);
//                    System.out.println("capture a image " + frameNr);
//                    //bufferedImage = ImageUtils.matToBufferedImage(image);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    logger.info("Exception during executing matToBufferedImage " + location);
//                    if (e instanceof CvException) {
//                    	capture.open(location);
//                    	continue;
//                    }
//                }
//
//                if (mat == null) continue;
//                Mat smallMat = new Mat();
//                Imgproc.resize(mat, smallMat, new Size(), 0.5, 0.5, Imgproc.INTER_LINEAR);
//                matbytes = ImageUtils.matToBytes(smallMat);
//                if (matbytes == null) continue;
//                
//                if (frameNr % frameSkip < groupSize) try {
//                    //byte[] buffer = ImageUtils.imageToBytes(bufferedImage, imageType);
//                    long timestamp = System.currentTimeMillis();
//                    if (frameMs > 0) timestamp = frameNr * frameMs;
//                    MatImage matImage = new MatImage(matbytes, streamId, smallMat.width(), smallMat.height(), timestamp, frameNr);
//                    matQueue.put(matImage);
//                    
////                    Frame newFrame = new Frame(streamId, frameNr, "mat", imageBytes, timestamp, new Rectangle(0, 0, image.width(), image.height()));
////                    //Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
////                    newFrame.getMetadata().put("uri", streamLocation);
////                    frameQueue.put(newFrame);
//                    // enforced throttling
//                    //if (sleepTime > 0) Utils.sleep(sleepTime);
//                    // queue based throttling
//                    //if (frameQueue.size() > (ZKConstant.SpoutQueueSize / 2)) Utils.sleep(frameQueue.size());
//                } catch (Exception e) {
//                    logger.warn("Unable to process new frame due to: " + e.getMessage(), e);
//                }
//                frameNr++;
//            }
//        } else {
//            logger.info("The location " + location + " can not be opened!");
//        }
//        capture.release();
//        logger.info("VideoCapture at " + location + " is released");
//        System.exit(0);
//    }

    /**
     * Tells the StreamReader to stop reading frames
     */
    public void stop() {
        running = false;
    }
    
    /**
     * Returns whether the StreamReader is still active or not
     *
     * @return
     */
    public boolean isRunning() {
        // kill this thread if the last frame read is to long ago (means Xuggler missed the EoF) and clear resources
        if (lastRead > 0 && System.currentTimeMillis() - lastRead > 3000) {
            running = false;
            return this.running;
        }
        return true;
    }
}

