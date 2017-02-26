package nl.tno.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import nl.tno.stormcv.bolt.SingleH264InputBolt;
import nl.tno.stormcv.operation.single.*;
import nl.tno.stormcv.spout.TCPCaptureSpout;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPCaptureTopology extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(TCPCaptureTopology.class);
    private String streamId;
    private String videoAddr;
    private String rtmpAddr;
    private String effect;
    private SourceInfo sourceInfo;

    public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        effect = "";
        conf.setNumWorkers(2);
    }

    public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr, String effect) {
        this.streamId = streamId;
        this.videoAddr = videoAddr;
        this.rtmpAddr = rtmpAddr;
        this.effect = effect;
        conf.setNumWorkers(6);
    }

    public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr, String effect, int workerNum) {
        this(streamId, rtmpAddr, videoAddr, effect);
        conf.setNumWorkers(workerNum);
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
        String source = "tcpSpout";
        if (effect != null && !effect.isEmpty()) {
            if (effect.equals("gray")) {
                builder.setBolt("gray", new SingleH264InputBolt(new GrayImageOp()),
                        4).shuffleGrouping(source);
                source = "gray";
            } else if (effect.equals("cannyEdge")) {
                builder.setBolt("cannyEdge",
                        new SingleH264InputBolt(new CannyEdgeOp("cannyEdge")), 4)
                        .shuffleGrouping(source);
                source = "cannyEdge";
            } else if (effect.equals("colorHistogram")) {
                builder.setBolt("colorHistogram",
                        new SingleH264InputBolt(new ColorHistogramOp(streamId)), 4)
                        .shuffleGrouping(source);
                source = "colorHistogram";
            } else if (effect.equals("foregroundExtraction")) {
                builder.setBolt("foregroundExtraction",
                        new SingleH264InputBolt(new FGExtranctionOp()), 1)
                        .shuffleGrouping(source);
                source = "foregroundExtraction";
                conf.setNumWorkers(3);
                conf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m -Xms2048m -Xmn2048m -XX:PermSize=1024m -XX:MaxPermSize=2048m -XX:-PrintGCDetails -XX:-PrintGCTimeStamps");
                conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192);
                conf.put(Config.WORKER_HEAP_MEMORY_MB, 4096);
                conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 4096);
                conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 1024);
            }
        }
        logger.info("rtmpAddr: " + rtmpAddr + ", streamId: " + streamId);

        // For codec test
//		builder.setBolt("codec", new SingleH264InputBolt(new CodecTestOperation()).setSourceInfo(sourceInfo),
//				1).shuffleGrouping(source);
//		source = "codec";

        // For debug
        builder.setBolt("debug", new SingleH264InputBolt(new EmptyOperation()).setSourceInfo(sourceInfo),
                1).shuffleGrouping(source);
        source = "debug";

        // For H264 rtmp operation
        builder.setBolt("streamer", new SingleH264InputBolt(new H264RtmpStreamOp().
                RTMPServer(rtmpAddr).
                appName(streamId)).setSourceInfo(sourceInfo), 1).shuffleGrouping(source);
        source = "streamer";

        // For debug
//		builder.setBolt("debug2", new SingleH264InputBolt(new EmptyOperation2()).setSourceInfo(sourceInfo),
//				1).shuffleGrouping(source);


        // for RTMP operation
//		builder.setBolt(
//				"streamer",
//				new SingleH264InputBolt(new SingleRTMPWriterOp()
//						.RTMPServer(rtmpAddr).appName(streamId).frameRate(25)),
//				1).shuffleGrouping(source);
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

}
