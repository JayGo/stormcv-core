package edu.fudan.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.operation.single.GrayImageOp;
import edu.fudan.stormcv.operation.single.H264RtmpStreamOp;
import edu.fudan.stormcv.spout.TCPCaptureSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayScaleTopologyH264 extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(GrayScaleTopologyH264.class);
    private String streamId = "GrayScaleTopologyH264";
    private String videoAddr = GlobalConstants.PseudoRtspAddress;
    private String rtmpAddr = GlobalConstants.DefaultRTMPServer;
    private SourceInfo sourceInfo;

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

        builder.setBolt("streamer", new SingleH264InputBolt(new H264RtmpStreamOp().
                RTMPServer(rtmpAddr).
                appName("grayscale")).setSourceInfo(sourceInfo), 1).localOrShuffleGrouping("gray");
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
