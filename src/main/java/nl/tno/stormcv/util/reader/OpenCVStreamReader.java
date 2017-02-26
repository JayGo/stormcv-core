package nl.tno.stormcv.util.reader;

import com.xuggle.mediatool.MediaListenerAdapter;
import nl.tno.stormcv.capture.BufferedImagePackQueue;
import nl.tno.stormcv.capture.BufferedImagePackQueue.BufferedImagePack;
import nl.tno.stormcv.capture.ThreadManager;
import nl.tno.stormcv.codec.JPEGImageCodec;
import nl.tno.stormcv.codec.TurboJPEGImageCodec;
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
import java.util.concurrent.LinkedBlockingQueue;

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
    private String streamId;
    private int frameSkip;
    private int groupSize;
    private long frameNr; // number of the frame read so far
    private boolean running = false; // indicator if the reader is still active
    private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
    private long lastRead = -1; // used to determine if the EOF was reached if Xuggler does not detect it
    private int sleepTime;
    private JPEGImageCodec codec;

    private String streamLocation;
    private String imageType = Frame.JPG_IMAGE;

    public OpenCVStreamReader(String streamId, String streamLocation, String imageType, int frameSkip, int groupSize, int sleepTime, LinkedBlockingQueue<Frame> frameQueue) {
        this.streamLocation = streamLocation;
        this.imageType = imageType;
        this.frameSkip = Math.max(1, frameSkip);
        this.groupSize = Math.max(1, groupSize);
        this.sleepTime = sleepTime;
        this.frameQueue = frameQueue;
        this.streamId = streamId;
        lastRead = System.currentTimeMillis() + 10000;
        this.codec = new TurboJPEGImageCodec();
    }

    @Override
    public void run() {
        running = true;
        startStreamReading();
        //startMultiThreadStreamReading();
    }

    public void stop() {
        running = false;
    }

    private void startMultiThreadStreamReading() {
        if (streamLocation == null) return;
        BufferedImagePackQueue mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
        ThreadManager.getInstance().startProcessingAndReading();
        frameNr = 0;
        while (running) {
            try {
                BufferedImagePack mBufferedImagePack = mBufferedImagePackQueue.pop(frameNr);
                if (mBufferedImagePack == null) {
                    continue;
                }
                BufferedImage bufferedImage = mBufferedImagePack.getImage();
                long timestamp = System.currentTimeMillis();
                byte[] bytes = codec.BufferedImageToJPEGBytes(bufferedImage);
                Frame newFrame = new Frame(streamId, frameNr, imageType, bytes, timestamp, new Rectangle(0, 0, bufferedImage.getWidth(), bufferedImage.getHeight()));
                //newFrame.getMetadata().put("uri", streamLocation);
                frameQueue.put(newFrame);
            } catch (Exception e) {
                e.printStackTrace();
            }
            frameNr++;
        }
    }

    private void startStreamReading() {
        if (streamLocation == null) return;
        VideoCapture capture = new VideoCapture();
        Mat mat = new Mat();
        frameNr = 0;
        long start = 0, end;

        capture.open(this.streamLocation);
        int width = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
        int height = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
        double frameRate = capture.get(5);
        logger.info(width + "x" + height + " , frameRate=" + frameRate);
        if (capture.isOpened()) {
            logger.info("Video is captured at " + streamLocation);
            byte[] imageBytes = null;
            while (running) {
                try {
                    capture.read(mat);
                    imageBytes = this.codec.MatToJPEGBytes(mat);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Exception during executing matToBufferedImage " + streamLocation);
                    if (e instanceof CvException) {
                        capture.open(streamLocation);
                        continue;
                    }
                }
                lastRead = System.currentTimeMillis();
                if (imageBytes == null) continue;
                if (frameNr % frameSkip < groupSize) try {
                    Frame newFrame = new Frame(streamId, frameNr, "mat", imageBytes, lastRead, new Rectangle(0, 0, mat.width(), mat.height()));
                    newFrame.getMetadata().put("uri", streamLocation);
                    frameQueue.put(newFrame);
                    if (sleepTime > 0) Utils.sleep(sleepTime);
                } catch (Exception e) {
                    logger.warn("Unable to process new frame due to: " + e.getMessage());
                }


                if (frameNr % 500 == 0) {
                    start = System.currentTimeMillis();
                }
                frameNr++;
                if (frameNr % 500 == 0) {
                    end = System.currentTimeMillis();
                    logger.info("OpenCVRtspReader Rate: {}", (500 / ((end - start) / 1000.0f)));
                }
            }
        }
        capture.release();
        logger.info("VideoCapture at " + streamLocation + " is released");
    }
}

