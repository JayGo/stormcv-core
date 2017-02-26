package nl.tno.stormcv.topology;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
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
public class GrayScaleTopology extends BaseTopology {

    private int frameSkip = 0;
    private List<String> urls;
    private String streamId = "GrayScaleTopology";

    public GrayScaleTopology() {
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
        builder.setBolt("scale", new SingleInputBolt(new ScaleImageOp(0.66f)),
                1).shuffleGrouping("spout");
        builder.setBolt("gray", new SingleInputBolt(new GrayscaleOp()), 1)
                .shuffleGrouping("scale");
        builder.setBolt(
                "streamer",
                new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
                        .maxSize(6), new MjpegStreamingOp().port(8558).framerate(24))
                        .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                .shuffleGrouping("scale");
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }

    public static void main(String[] args) {
        GrayScaleTopology topology = new GrayScaleTopology();
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
