package nl.tno.stormcv.util.reader;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.event.IVideoPictureEvent;

import nl.tno.stormcv.capture.BufferedImagePackQueue;
import nl.tno.stormcv.capture.ThreadManager;
import nl.tno.stormcv.capture.BufferedImagePackQueue.BufferedImagePack;
import nl.tno.stormcv.codec.JPEGImageCodec;
import nl.tno.stormcv.codec.TurboJPEGImageCodec;
import nl.tno.stormcv.model.Frame;

import nl.tno.stormcv.util.ImageUtils;
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
 * The XuggleRTSPReaderTest will automatically throttle itself based on the size of the queue it writes the frames to.
 * This is done to avoid memory overload if production of frames is higher than the consumption. The actual decoding of
 * frames is done by Xuggler which in turn uses FFMPEG (xuggler jar file is shipped with ffmpeg binaries).
 *
 * @author Corne Versloot
 */
public class OpenCVStreamReader extends MediaListenerAdapter implements Runnable {

    private Logger logger = LoggerFactory.getLogger(OpenCVStreamReader.class);
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
    private int frameMs = -1;
    private JPEGImageCodec codec;

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
        this.codec = TurboJPEGImageCodec.getInstance();
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
        this.codec = TurboJPEGImageCodec.getInstance();
    }

    @Override
	public void run() {
        startMultiThreadStreamReading(streamLocation);
    }

    public void stop() {
        running = false;
    }

    public void startMultiThreadStreamReading(String location) {
    	BufferedImagePackQueue mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
    	ThreadManager.getInstance().startProcessingAndReading();
    	frameNr = 0;
        while (true) {
            try {
    			BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(frameNr);
    			if(mBufferedImagePack == null) {
    				continue;
    			}
    			BufferedImage bufferedImage = mBufferedImagePack.getImage();

//            	byte[] buffer = ImageUtils.imageToBytes(bufferedImage, imageType);
//            	byte[] buffer = {1,0,1,0};
                long timestamp = System.currentTimeMillis();
                byte[] bytes = codec.BufferedImageToJPEGBytes(bufferedImage);
                Frame newFrame = new Frame(streamId, frameNr, imageType, bytes, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
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

    public void startStreamReading(String location) {
    	VideoCapture capture = new VideoCapture();
        Mat image = new Mat();
        frameNr = 0;
        capture.open(location);
        int width = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
        int height = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
        double frameRate = capture.get(5);
        logger.info(width + "x" + height + " , frameRate=" + frameRate);
        if (capture.isOpened()) {
            logger.info("Video is captured at " + location);
            byte[] imageBytes = null;
            while (true) {
                try {
                    capture.read(image);
                    imageBytes = ImageUtils.matToBytes(image);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Exception during executing matToBufferedImage " + location);
                    if (e instanceof CvException) {
                    	capture.open(location);
                    	continue;
                    }
                }

                lastRead = System.currentTimeMillis();
                if (imageBytes == null) continue;
                if (frameNr % frameSkip < groupSize) try {
                    long timestamp = lastRead;
                    if (frameMs > 0) timestamp = frameNr * frameMs;
                    Frame newFrame = new Frame(streamId, frameNr, "mat", imageBytes, timestamp, new Rectangle(0, 0, image.width(), image.height()));
                    newFrame.getMetadata().put("uri", streamLocation);
                    frameQueue.put(newFrame);

                    if (sleepTime > 0) Utils.sleep(sleepTime);
                } catch (Exception e) {
                    logger.warn("Unable to process new frame due to: " + e.getMessage(), e);
                }
                frameNr++;
            }
        } else {
            logger.info("The location " + location + " can not be opened!");
        }
        capture.release();
        logger.info("VideoCapture at " + location + " is released");
    }
}

