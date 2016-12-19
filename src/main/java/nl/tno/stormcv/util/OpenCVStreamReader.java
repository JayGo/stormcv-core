package nl.tno.stormcv.util;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;

import nl.tno.stormcv.capture.BufferedImagePackQueue;
import nl.tno.stormcv.capture.MatPackQueue;
import nl.tno.stormcv.capture.ThreadManager;
import nl.tno.stormcv.capture.BufferedImagePackQueue.BufferedImagePack;
import nl.tno.stormcv.model.Frame;

import org.apache.storm.utils.Utils;
import org.opencv.core.CvException;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class reads a video stream or file, decodes frames and puts those in a queue for further processing.
 * The StreamReader will automatically throttle itself based on the size of the queue it writes the frames to.
 * This is done to avoid memory overload if production of frames is higher than the consumption. The actual decoding of
 * frames is done by Xuggler which in turn uses FFMPEG (xuggler jar file is shipped with ffmpeg binaries).
 *
 * @author Corne Versloot
 */
public class OpenCVStreamReader extends MediaListenerAdapter implements Runnable {

    private Logger logger = LoggerFactory.getLogger(OpenCVStreamReader.class);
    private IMediaReader mediaReader;
    private int mVideoStreamIndex = -1;
    private String streamId;
    private int frameSkip;
    private int groupSize;
    private long frameNr; // number of the frame read so far
    private boolean running = false; // indicator if the reader is still active
    private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
    private long lastRead = -1; // used to determine if the EOF was reached if Xuggler does not detect it
    private int sleepTime;
    private boolean useSingleID = false;
    private String tmpDir;
    private int frameMs = -1;

    private String streamLocation;
    private LinkedBlockingQueue<String> videoList = null;
    private String imageType = Frame.JPG_IMAGE;

    public OpenCVStreamReader(LinkedBlockingQueue<String> videoList, String imageType, int frameSkip, int groupSize, int sleepTime, boolean uniqueIdPerFile, LinkedBlockingQueue<Frame> frameQueue) {
        this.videoList = videoList;
        this.imageType = imageType;
        this.frameSkip = Math.max(1, frameSkip);
        this.groupSize = Math.max(1, groupSize);
        this.sleepTime = sleepTime;
        this.frameQueue = frameQueue;
        this.useSingleID = uniqueIdPerFile;
        this.streamId = "" + this.hashCode(); // give a default streamId
        lastRead = System.currentTimeMillis() + 10000;
        try {
            tmpDir = File.createTempFile("abc", ".tmp").getParent();
        } catch (IOException e) {
        }
    }

    public OpenCVStreamReader(String streamId, String streamLocation, String imageType, int frameSkip, int groupSize, int sleepTime, LinkedBlockingQueue<Frame> frameQueue) {
        this.streamLocation = streamLocation;
        this.imageType = imageType;
        this.frameSkip = Math.max(1, frameSkip);
        this.groupSize = Math.max(1, groupSize);
        this.sleepTime = sleepTime;
        this.frameQueue = frameQueue;
        this.streamId = streamId;
        lastRead = System.currentTimeMillis() + 10000;
        try {
            tmpDir = File.createTempFile("abc", ".tmp").getParent();
        } catch (IOException e) {
        }
    }

    /*
     * Start reading the provided URL
     */
    @Override
	public void run() {
    	startStreamReading(streamLocation);
//        running = true;
//        while (running) {
//            try {
//                // if a url was provided read it
//                if (videoList == null && streamLocation != null) {
//                    logger.info("Start reading stream: " + streamLocation);
//                    startStreamReading(streamLocation);
//                    //mediaReader = ToolFactory.makeReader(streamLocation);
//                } else if (videoList != null) {
//                    // read next video from the list or block until one is available
//                    logger.info("Waiting for new file to be downloaded...");
//                    streamLocation = videoList.take();
//
//                    if (!useSingleID) {
//                        streamId = "" + streamLocation.hashCode();
//                        if (streamLocation.contains("/"))
//                            streamId = streamLocation.substring(streamLocation.lastIndexOf('/') + 1) + "_" + streamId;
//                    }
//                    logger.info("Start reading File: " + streamLocation);
//
//                    // read framerate from file
//                    IContainer cont = IContainer.make();
//                    if (cont.open(streamLocation, IContainer.Type.READ, null) > 0) try {
//                        for (int s = 0; s < cont.getNumStreams(); s++) {// find the videostream
//                            if (cont.getStream(s).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
//                                frameMs = (int) Math.floor(1000f / cont.getStream(s).getStreamCoder().getFrameRate().getDouble());
//                            }
//                        }
//                    } catch (Exception e) {
//                        logger.warn("Unable to read metadata from video");
//                    }
//
//                    mediaReader = ToolFactory.makeReader(streamLocation);
//
//                    lastRead = System.currentTimeMillis() + 10000;
//
//                    mVideoStreamIndex = -1;
//                    if (!useSingleID) frameNr = 0;
//                    mediaReader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
//                    mediaReader.addListener(this);
//
//                    while (mediaReader.readPacket() == null && running) ;
//
//                    // reset internal state
//                    mediaReader.close();
//                    // delete the local file (if it is in tmp dir)
//                    if (videoList != null) {
//                        File localFile = new File(streamLocation);
//                        if (localFile.getAbsolutePath().startsWith(tmpDir)) {
//                            localFile.delete();
//                        }
//                    }
//                } else {
//                    logger.error("No video list or url provided, nothing to read");
//                    break;
//                }
//            } catch (Exception e) {
//                logger.warn("Stream closed unexpectatly: " + e.getMessage(), e);
//                // sleep a minute and try to read the stream again
//                Utils.sleep(1 * 10 * 1000);  // have change to 10s for test
//            }
//        }
//
//        mVideoStreamIndex = -1;
//        running = false;
    }

    public void startStreamReading(String location) {
    	
    	BufferedImagePackQueue mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
    	ThreadManager.getInstance().startProcessingAndReading();
    	frameNr = 0;
        while (true) {
            try {
            	
    			BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(frameNr);
    			
    			if(mBufferedImagePack == null) {
    				continue;
    			}
////    			System.out.println("spout frame " + frameNr);
    			BufferedImage bufferedImage = mBufferedImagePack.getImage();
    			
//            	byte[] buffer = ImageUtils.imageToBytes(bufferedImage, imageType);
//            	byte[] buffer = {1,0,1,0};
                long timestamp = System.currentTimeMillis();
                Frame newFrame = new Frame(streamId, frameNr, imageType, bufferedImage, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
//                Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, 1, 1));
//                Frame newFrame = new Frame(streamId, frameNr, imageType, bufferedImage, timestamp, new Rectangle(0, 0, 1, 1));
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
//        Mat image = new Mat();
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
//        //BufferedImage bufferedImage = null;
//        
//        if (capture.isOpened()) {
//            logger.info("Video is captured at " + location);
//            byte[] imageBytes = null;
//            while (true) {
//                try {
//                    //capture.retrieve(image); //same results as .read()
//                    capture.read(image);
//                    imageBytes = ImageUtils.matToBytes(image);
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
//                lastRead = System.currentTimeMillis();
//                //if (bufferedImage == null) continue;  //check
//                if (imageBytes == null) continue;
//                
//                if (frameNr % frameSkip < groupSize) try {
//                    //byte[] buffer = ImageUtils.imageToBytes(bufferedImage, imageType);
//                    long timestamp = lastRead;
//                    if (frameMs > 0) timestamp = frameNr * frameMs;
//                    Frame newFrame = new Frame(streamId, frameNr, "mat", imageBytes, timestamp, new Rectangle(0, 0, image.width(), image.height()));
//                    //Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
//                    newFrame.getMetadata().put("uri", streamLocation);
//                    frameQueue.put(newFrame);
//                    // enforced throttling
//                    if (sleepTime > 0) Utils.sleep(sleepTime);
//                    // queue based throttling
//                    //if (frameQueue.size() > (Constant.SpoutQueueSize / 2)) Utils.sleep(frameQueue.size());                    
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
     * Gets called when FFMPEG transcoded a frame
     */
    @Override
	public void onVideoPicture(IVideoPictureEvent event) {
        lastRead = System.currentTimeMillis();
        if (event.getStreamIndex() != mVideoStreamIndex) {
            if (mVideoStreamIndex == -1) {
                mVideoStreamIndex = event.getStreamIndex();
            } else return;
        }
        if (frameNr % frameSkip < groupSize) try {
            BufferedImage frame = event.getImage();
            byte[] buffer = ImageUtils.imageToBytes(frame, imageType);
            long timestamp = event.getTimeStamp(TimeUnit.MILLISECONDS);
            if (frameMs > 0) timestamp = frameNr * frameMs;
            Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, frame.getWidth(), frame.getHeight()));
            newFrame.getMetadata().put("uri", streamLocation);
            frameQueue.put(newFrame);
            // logger.info("read one frame at " + lastRead);
            // enforced throttling
            if (sleepTime > 0) Utils.sleep(sleepTime);
            // queue based throttling
            if (frameQueue.size() > 20) Utils.sleep(frameQueue.size());
        } catch (Exception e) {
            logger.warn("Unable to process new frame due to: " + e.getMessage(), e);
        }

        frameNr++;
    }


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
            if (mediaReader != null && mediaReader.getContainer() != null) mediaReader.getContainer().close();
            return this.running;
        }
        return true;
    }

}

