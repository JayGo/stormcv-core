package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchJPEGInputBolt;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.fetcher.OpenCVStreamFrameFetcher;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.spout.CVParticleSpout;
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
    private boolean view720p = false;

    public GrayScaleTopologyJPEG() {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        urls = new ArrayList<String>();
        if (view720p) {
            urls.add(GlobalConstants.Pseudo720pRtspAddress);
        } else {
            urls.add(GlobalConstants.PseudoRtspAddress);
        }
        //urls.add("/home/nfs/videos/grass1080.mp4");
        isTopologyRunningAtLocal = true;
    }

    @Override
    public void setSpout() {
        builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
                urls).frameSkip(frameSkip)), 1);
    }

    @Override
    public void setBolts() {
//        builder.setBolt("scale", new SingleJPEGInputBolt(new ScaleImageOp(0.5f), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE),
//                1).localOrShuffleGrouping("spout");
//        builder.setBolt("gray", new SingleJPEGInputBolt(new GrayImageOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT), 1)
//                .localOrShuffleGrouping("scale");
//        builder.setBolt("histogram", new SingleJPEGInputBolt(new ColorHistogramOp(streamId).outputFrame(true),
//                        BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("spout");

//        builder.setBolt("facedetect", new SingleJPEGInputBolt(new HaarCascadeOp(streamId, "lbpcascade_frontalface.xml").outputFrame(true),
//                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("spout");
//
//        builder.setBolt("drawer", new SingleJPEGInputBolt(new DrawFeaturesOp(),
//                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("facedetect");
        boolean sendRtmp = false;
        if (!sendRtmp) {
            builder.setBolt(
                    "streamer",
                    new BatchJPEGInputBolt(new SlidingWindowBatcher(2, frameSkip)
                            .maxSize(6), new MjpegStreamingOp().useMat(false).port(8558).framerate(24),
                            BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE)
                            .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                    .localOrShuffleGrouping("drawer");
        } else {
            builder.setBolt("streamer", new SingleJPEGInputBolt(new SingleRTMPWriterOp().appName("grayscale")
                    .frameRate(23.98).bitRate(886000), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("drawer");
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
