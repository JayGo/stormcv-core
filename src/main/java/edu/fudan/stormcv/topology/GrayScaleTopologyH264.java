package edu.fudan.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchH264InputBolt;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.GrayImageOp;
import edu.fudan.stormcv.spout.TCPCaptureSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayScaleTopologyH264 extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(GrayScaleTopologyH264.class);
    private String streamId = "GrayScaleTopologyH264";
    private String videoAddr = GlobalConstants.PseudoRtspAddress;
    //private String videoAddr = "/home/nfs/videos/grass1080.mp4";
    private String rtmpAddr = GlobalConstants.DefaultRTMPServer;
    private SourceInfo sourceInfo;
    private boolean sendRtmp = false;
    private int frameSkip = 0;

    public GrayScaleTopologyH264() {
        conf.setNumWorkers(2);
        isTopologyRunningAtLocal = true;
    }

    public GrayScaleTopologyH264(String streamId, String rtmpAddr, String videoAddr) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        conf.setNumWorkers(2);
    }

    @Override
    public void setSpout() {
        TCPCaptureSpout tcpCaptureSpout = new TCPCaptureSpout(streamId, videoAddr, CodecType.CODEC_TYPE_H264_CPU);
        builder.setSpout("tcpSpout", tcpCaptureSpout);
        sourceInfo = tcpCaptureSpout.getSourceInfo();
        if (sourceInfo == null) {
            logger.info("sourceInfo returned is null! System exit!");
            System.exit(-1);
        }
    }

    @Override
    public void setBolts() {
        builder.setBolt("gray", new SingleH264InputBolt(new GrayImageOp()).setSourceInfo(sourceInfo),
                1).localOrShuffleGrouping("tcpSpout");
////        builder.setBolt("streamer", new SingleH264InputBolt(new H264RtmpStreamOp().
////                RTMPServer(rtmpAddr).
////                appName("grayscale")).setSourceInfo(sourceInfo), 1).localOrShuffleGrouping("gray");
        builder.setBolt(
                "streamer",
                new BatchH264InputBolt(new SlidingWindowBatcher(2, frameSkip)
                        .maxSize(6), new MjpegStreamingOp().useMat(false).port(8558).framerate(24)).setSourceInfo(sourceInfo)
                        .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                .localOrShuffleGrouping("gray");
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    public static void main(String[] args) {
        LibLoader.loadOpenCVLib();
        GrayScaleTopologyH264 topology = new GrayScaleTopologyH264();
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
