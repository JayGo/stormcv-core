package edu.fudan.stormcv.util.reader;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.*;
import edu.fudan.stormcv.codec.JPEGImageCodec;
import edu.fudan.stormcv.codec.TurboJPEGImageCodec;
import org.apache.storm.utils.Utils;
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
public class XugglerStreamReader extends MediaListenerAdapter implements Runnable {
    private Logger logger = LoggerFactory.getLogger(XugglerStreamReader.class);
    private int streamIndex = -1;
    private String streamId;
    private int frameSkip;
    private int groupSize;
    private long frameNr; // number of the frame read so far
    private boolean running = false; // indicator if the reader is still active
    private LinkedBlockingQueue<edu.fudan.stormcv.model.Frame> frameQueue; // queue used to store frames
    private int sleepTime;
    private String streamLocation;
    private String imageType = edu.fudan.stormcv.model.Frame.JPG_IMAGE;
    private JPEGImageCodec codec;

    public XugglerStreamReader(String streamId, String streamLocation, String imageType, int frameSkip, int groupSize, int sleepTime, LinkedBlockingQueue<edu.fudan.stormcv.model.Frame> frameQueue) {
        this.streamLocation = streamLocation;
        this.imageType = imageType;
        this.frameSkip = Math.max(1, frameSkip);
        this.groupSize = Math.max(1, groupSize);
        this.sleepTime = sleepTime;
        this.frameQueue = frameQueue;
        this.streamId = streamId;
        this.codec = new TurboJPEGImageCodec();
    }

    @Override
    public void run() {
        runDefaultReader();
    }

    @Override
    public void onVideoPicture(IVideoPictureEvent event) {
        if (event.getStreamIndex() != streamIndex) {
            if (streamIndex == -1) {
                streamIndex = event.getStreamIndex();
            } else return;
        }
        processFrame(event.getImage());
    }

    public void stop() {
        running = false;
    }

    private void runDefaultReader() {
        IMediaReader mediaReader = ToolFactory.makeReader(streamLocation);
        streamIndex = -1;
        mediaReader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
        mediaReader.addListener(this);
        while (mediaReader.readPacket() == null && running) ;
        mediaReader.close();
        logger.info("stop reading stream");
    }

    private void runCustomReader() {
        IContainer container = IContainer.make();
        if (container.open(streamLocation, IContainer.Type.READ, null) < 0)
            throw new IllegalArgumentException("could not open file: " + streamLocation);
        int numStreams = container.getNumStreams();
        int videoStreamId = -1;
        IStreamCoder videoCoder = null;
        for (int i = 0; i < numStreams; i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                videoStreamId = i;
                videoCoder = coder;
                break;
            }
        }
        if (videoStreamId == -1)
            throw new RuntimeException("could not find video stream in container: "
                    + streamLocation);
        if (videoCoder.open() < 0)
            throw new RuntimeException("could not open video decoder for container: "
                    + streamLocation);
        IPacket packet = IPacket.make();
        long firstTimestampInStream = Global.NO_PTS;
        long systemClockStartTime = 0;
        while (container.readNextPacket(packet) >= 0 && running) {
            if (packet.getStreamIndex() == videoStreamId) {
                IVideoPicture picture = IVideoPicture.make(videoCoder.getPixelType(),
                        videoCoder.getWidth(), videoCoder.getHeight());
                int offset = 0;
                while (offset < packet.getSize()) {
                    int bytesDecoded = videoCoder.decodeVideo(picture, packet, offset);
                    if (bytesDecoded < 0)
                        throw new RuntimeException("got error decoding video in: "
                                + streamLocation);
                    offset += bytesDecoded;

                    if (picture.isComplete()) {
                        IVideoPicture newPic = picture;
                        if (firstTimestampInStream == Global.NO_PTS) {
                            firstTimestampInStream = picture.getTimeStamp();
                            systemClockStartTime = System.currentTimeMillis();
                        } else {
                            long systemClockCurrentTime = System.currentTimeMillis();
                            long millisecondsClockTimeSinceStartofVideo =
                                    systemClockCurrentTime - systemClockStartTime;
                            // compute how long for this frame since the first frame in the
                            // stream.
                            // remember that IVideoPicture and IAudioSamples timestamps are
                            // always in MICROSECONDS,
                            // so we divide by 1000 to get milliseconds.
                            long millisecondsStreamTimeSinceStartOfVideo =
                                    (picture.getTimeStamp() - firstTimestampInStream) / 1000;
                            final long millisecondsTolerance = 50; // and we give ourselfs 50 ms of tolerance
                            final long millisecondsToSleep =
                                    (millisecondsStreamTimeSinceStartOfVideo -
                                            (millisecondsClockTimeSinceStartofVideo +
                                                    millisecondsTolerance));
                            if (millisecondsToSleep > 0) {
                                try {
                                    Thread.sleep(millisecondsToSleep);
                                } catch (InterruptedException e) {
                                    // we might get this when the user closes the dialog box, so
                                    // just return from the method.
                                    return;
                                }
                            }
                        }
                        BufferedImage image = com.xuggle.xuggler.Utils.videoPictureToImage(newPic);
                        processFrame(image);
                    }
                }
            }
        }
        videoCoder.close();
        container.close();
    }

    private void processFrame(BufferedImage image) {
        if (frameNr % frameSkip < groupSize) {
            byte[] buffer = this.codec.BufferedImageToJPEGBytes(image);
            long timestamp = System.currentTimeMillis();
            edu.fudan.stormcv.model.Frame newFrame = new edu.fudan.stormcv.model.Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0, image.getWidth(), image.getHeight()));
            newFrame.getMetadata().put("uri", streamLocation);
            try {
                frameQueue.put(newFrame);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (sleepTime > 0) Utils.sleep(sleepTime);
            // queue based throttling
            if (frameQueue.size() > 20) Utils.sleep(frameQueue.size());
        }
        frameNr++;
    }
}

