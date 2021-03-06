package edu.fudan.stormcv.fetcher;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.GroupOfFrames;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.model.serializer.GroupOfFramesSerializer;
import edu.fudan.stormcv.operation.single.GroupOfFramesOp;
import edu.fudan.stormcv.util.reader.TCPRawStreamReader;
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
public class RawStreamFrameFetcher implements IFetcher<CVParticle> {

    private Logger logger = LoggerFactory
            .getLogger(RawStreamFrameFetcher.class);
    private static final long serialVersionUID = -6394106024444514811L;
    protected List<String> locations;
    protected int frameSkip = 1;
    private int groupSize = 1;
    protected LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(
            20);
    protected Map<String, TCPRawStreamReader> rawStreamReaders;
    private int sleepTime = 0;
    private String imageType;
    private int batchSize = 1;
    private List<Frame> frameGroup;

    public RawStreamFrameFetcher(List<String> locations) {
        this.locations = locations;
    }

    public RawStreamFrameFetcher frameSkip(int skip) {
        this.frameSkip = skip;
        return this;
    }

    /**
     * Sets the number of frames for a group
     *
     * @param size
     * @return
     */
    public RawStreamFrameFetcher groupSize(int size) {
        this.groupSize = size;
        return this;
    }

    public RawStreamFrameFetcher sleep(int ms) {
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
    public RawStreamFrameFetcher groupOfFramesOutput(int nrFrames) {
        this.batchSize = nrFrames;
        return this;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void prepare(Map conf, TopologyContext context) throws Exception {

        int nrTasks = context.getComponentTasks(context.getThisComponentId())
                .size();
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
        if (rawStreamReaders != null) {
            this.deactivate();
        }
        rawStreamReaders = new HashMap<String, TCPRawStreamReader>();
        for (String location : locations) {
            String[] address = location.split(":");
            if (address.length < 2) {
                logger.warn("unable to read the correct ip and port;");
            }
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            int bufferSize = 10240000;

            String streamId = ip + "-" + location.hashCode();
            TCPRawStreamReader TCPRawStreamReader = new TCPRawStreamReader(ip, port,
                    bufferSize, streamId, imageType, frameSkip, groupSize,
                    sleepTime, frameQueue);
            rawStreamReaders.put(location, TCPRawStreamReader);
            new Thread(TCPRawStreamReader).start();
        }
    }

    @Override
    public void deactivate() {
        if (rawStreamReaders != null) {
            for (String location : rawStreamReaders.keySet()) {
                rawStreamReaders.get(location).stopStreamReader();
            }
        }
        rawStreamReaders = null;
    }

    @Override
    public CVParticle fetchData() {
        if (rawStreamReaders == null)
            this.activate();
        Frame frame = frameQueue.poll();
        if (frame != null) {
            if (batchSize <= 1) {
                return frame;
            } else {
                if (frameGroup == null || frameGroup.size() >= batchSize)
                    frameGroup = new ArrayList<Frame>();
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
