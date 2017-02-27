package nl.tno.stormcv.topology;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleJPEGInputBolt;
import nl.tno.stormcv.constant.BOLT_HANDLE_TYPE;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.batch.MjpegStreamingOp;
import nl.tno.stormcv.operation.single.GrayImageOp;
import nl.tno.stormcv.operation.single.ScaleImageOp;
import nl.tno.stormcv.operation.single.SingleRTMPWriterOp;
import nl.tno.stormcv.spout.CVParticleSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:24 AM
 * Description:
 */
public class GrayScaleTopologyJPEG extends BaseTopology {

    private int frameSkip = 0;
    private List<String> urls;
    private String streamId = "GrayScaleTopologyJPEG";

    public GrayScaleTopologyJPEG() {
        conf.setNumWorkers(1);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        urls = new ArrayList<String>();
        urls.add(GlobalConstants.PseudoRtspAddress);
        isTopologyRunningAtLocal = true;
    }

    @Override
    public void setSpout() {
        builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
                urls).frameSkip(frameSkip)), 1);
    }

    @Override
    public void setBolts() {
        builder.setBolt("scale", new SingleJPEGInputBolt(new ScaleImageOp(0.5f), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE),
                1).localOrShuffleGrouping("spout");
        builder.setBolt("gray", new SingleJPEGInputBolt(new GrayImageOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT), 1)
                .localOrShuffleGrouping("scale");
        boolean sendRtmp = true;
        if (!sendRtmp) {
            builder.setBolt(
                    "streamer",
                    new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
                            .maxSize(6), new MjpegStreamingOp().port(8558).framerate(24))
                            .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                    .localOrShuffleGrouping("gray");
        } else {
            builder.setBolt("streamer", new SingleJPEGInputBolt(new SingleRTMPWriterOp().appName("grayscale")
                    .frameRate(24), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("gray");
        }
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }

    public static void main(String[] args) {
        GrayScaleTopologyJPEG topology = new GrayScaleTopologyJPEG();
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
