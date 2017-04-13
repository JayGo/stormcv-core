package edu.fudan.stormcv.fetcher;

import edu.fudan.stormcv.codec.TurboImageCodec;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This {@link IFetcher} implementation reads images that refresh constantly. Each url provided will be read
 * each SLEEP milliseconds. Each image will be emitted into the topology as a {@link Frame} object. How often
 * the image is read can be controlled by setting the sleep time (default = 40 ms)
 *
 * @author Corne Versloot
 */
public class RefreshingImageFetcher implements IFetcher<Frame> {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
    private int sleep = 40;
    private List<String> locations;
    private List<ImageReader> readers;
    private String imageType;

    public RefreshingImageFetcher(List<String> locations) {
        this.locations = locations;
    }

    /**
     * Determines how often this Fetcher reads and decodes an image. The default sleep is
     * 40 ms.
     *
     * @param ms
     * @return
     */
    public RefreshingImageFetcher sleep(int ms) {
        this.sleep = ms;
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        frameQueue = new LinkedBlockingQueue<Frame>();

        if (stormConf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)) {
            imageType = (String) stormConf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
        }

        int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int taskIndex = context.getThisTaskIndex();

        // change the list based on the number of tasks working on it
        if (this.locations != null && this.locations.size() > 0) {
            int batchSize = (int) Math.floor(locations.size() / nrTasks) + 1;
            int start = batchSize * taskIndex;
            locations = locations.subList(start, Math.min(start + batchSize, locations.size()));
        }
        readers = new ArrayList<>();
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return new FrameSerializer();
    }

    @Override
    public void activate() {
        for (String location : locations) {
            try {
                ImageReader ir = new ImageReader(new URL(location), sleep, frameQueue);
                new Thread(ir).start();
                readers.add(ir);
            } catch (MalformedURLException e) {
                logger.warn(location + " is not a valid URL!");
            }
        }
    }

    @Override
    public void deactivate() {
        for (ImageReader reader : readers) {
            reader.stop();
        }
        readers.clear();
        frameQueue.clear();
    }

    @Override
    public Frame fetchData() {
        return frameQueue.poll();
    }

    @Override
    public void onSignal(byte[] bytes) {

    }

    private class ImageReader implements Runnable {

        private Logger logger = LoggerFactory.getLogger(getClass());
        private LinkedBlockingQueue<Frame> frameQueue;
        private URL url;
        private int sleep;
        private int sequenceNr;
        private boolean running = true;
        private ImageCodec codec;

        public ImageReader(URL url, int sleep, LinkedBlockingQueue<Frame> frameQueue) {
            this.url = url;
            this.sleep = sleep;
            this.frameQueue = frameQueue;
            this.codec = new TurboImageCodec();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    BufferedImage image = ImageIO.read(url);
                    byte[] buffer = this.codec.BufferedImageToBytes(image, imageType);
                    Frame frame = new Frame(url.getFile().substring(1), sequenceNr, imageType, buffer, System.currentTimeMillis(), new Rectangle(image.getWidth(), image.getHeight()));
                    frame.getMetadata().put("uri", url);
                    frameQueue.put(frame);
                    sequenceNr++;
                    if (frameQueue.size() > 20) Utils.sleep(frameQueue.size());
                } catch (Exception e) {
                    logger.warn("Exception while reading " + url + " : " + e.getMessage());
                }
                Utils.sleep(sleep);
            }
        }

        public void stop() {
            this.running = false;
        }

    }

}
