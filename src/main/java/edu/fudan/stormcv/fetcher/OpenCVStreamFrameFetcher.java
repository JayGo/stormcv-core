package edu.fudan.stormcv.fetcher;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.GroupOfFrames;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.model.serializer.GroupOfFramesSerializer;
import edu.fudan.stormcv.operation.single.GroupOfFramesOp;
import edu.fudan.stormcv.util.reader.OpenCVStreamReader;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link IFetcher} implementation that reads video streams (either live or
 * not). The XugglerStreamFrameFetcher is initialized with a set of url's it must read.
 * These url's are divided among all StreamFrameFetchers in the topology. So if
 * the number of urls is larger than then number of Fetchers some of them will
 * read and decode multiple streams in parallel. Note that this can become a
 * problem if the number of streams read by the single spout consumes to many
 * resources (network and/or cpu).
 * <p>
 * The frameSkip and groupSize parameters define which frames will be extracted
 * using: frameNr % frameSkip < groupSize-1 With a frameSkip of 10 and groupSize
 * of 2 the following framenumbers will be extracted: 0,1,10,11,20,21,.. Both
 * frameskip and groupSize have default value 1 which means that all frames are
 * read.
 * <p>
 * It is possible to provide an additional sleep which is enforced after each
 * emitted frame. This sleep can be used to throttle the XugglerStreamFrameFetcher when
 * it is reading streams to fast (i.e. faster than topology can process). Use of
 * the sleep should be avoided when possible and throttling of the topology
 * should be done using the MAX_SPOUT_PENDING configuration parameter.
 * <p>
 * This fetcher can be configured to emit {@link GroupOfFrames} objects instead
 * of {@link Frame} by using the groupOfFramesOutput method. Emitting a
 * {@link GroupOfFrames} can be useful when the subsequent Operation requires
 * multiple frames of the same stream in which case the {@link GroupOfFramesOp}
 * can be used without a batcher.
 *
 * @author Corne Versloot
 */
public class OpenCVStreamFrameFetcher implements IFetcher<CVParticle> {

    private static final long serialVersionUID = 7135270229614102711L;
    
    private Logger logger = LoggerFactory.getLogger(OpenCVStreamFrameFetcher.class);
    protected List<String> locations;
    protected int frameSkip = 1;
    private int groupSize = 1;
    protected LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(
            GlobalConstants.SpoutQueueSize);
    protected Map<String, OpenCVStreamReader> streamReaders;
    private int sleepTime = 0;
    private String imageType;
    private int batchSize = 1;
    private List<Frame> frameGroup;
    private String id;

    public OpenCVStreamFrameFetcher(List<String> locations) {
        this.locations = locations;
    }

    public OpenCVStreamFrameFetcher frameSkip(int skip) {
        this.frameSkip = skip;
        return this;
    }

    /**
     * Sets the number of frames for a group
     *
     * @param size
     * @return
     */
    public OpenCVStreamFrameFetcher groupSize(int size) {
        this.groupSize = size;
        return this;
    }

    public OpenCVStreamFrameFetcher sleep(int ms) {
        this.sleepTime = ms;
        return this;
    }

    /**
     * Specifies the number of frames to be send at once. If set to 1 (default
     * value) this Fetcher will emit {@link Frame} objects. If set to 2 or more
     * it will emit {@link GroupOfFrames} objects.
     *
     * @param nrFrames
     * @return
     */
    public OpenCVStreamFrameFetcher groupOfFramesOutput(int nrFrames) {
        this.batchSize = nrFrames;
        return this;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void prepare(Map conf, TopologyContext context) throws Exception {
        this.id = context.getThisComponentId();
        int nrTasks = context.getComponentTasks(id).size();
        int taskIndex = context.getThisTaskIndex();

        if (conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)) {
            imageType = (String) conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
        }

        // change the list based on the number of tasks working on it
        if (this.locations != null && this.locations.size() > 0) {
            int batchSize = (int) Math.floor(locations.size() / nrTasks);
            int start = batchSize * taskIndex;
            locations = locations.subList(start,
                    Math.min(start + batchSize, locations.size()));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public CVParticleSerializer getSerializer() {
        if (batchSize <= 1)
            return new FrameSerializer();
        else
            return new GroupOfFramesSerializer();
    }

    @Override
    public void activate() {
        if (streamReaders != null) {
            this.deactivate();
        }
        streamReaders = new HashMap<String, OpenCVStreamReader>();
        for (String location : locations) {

            String streamId = "" + location.hashCode();
            if (location.contains("/")) {
                streamId = id + "_"
                        + location.substring(location.lastIndexOf("/") + 1)
                        + "_" + streamId;
            }
            OpenCVStreamReader reader = new OpenCVStreamReader(streamId,
                    location, imageType, frameSkip, groupSize, sleepTime,
                    frameQueue);
            streamReaders.put(location, reader);
            new Thread(reader).start();
        }
    }

    @Override
    public void deactivate() {
        if (streamReaders != null)
            for (String location : streamReaders.keySet()) {
                streamReaders.get(location).stop();
            }
        streamReaders = null;
    }

    @Override
    public CVParticle fetchData() {
        if (streamReaders == null)
            this.activate();
        Frame frame = frameQueue.poll();
 
        if (frame != null) {
            if (batchSize <= 1) {

                return frame;
            } else {
                if (frameGroup == null || frameGroup.size() >= batchSize)
                    frameGroup = new ArrayList<>();
                frameGroup.add(frame);
                if (frameGroup.size() == batchSize) {
                    return new GroupOfFrames(frameGroup.get(0).getStreamId(),
                            frameGroup.get(0).getSequenceNr(), frameGroup);
                }
            }
        }
        return null;
    }

    @Override
    public void onSignal(byte[] bytes) {

    }
}
