package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.bolt.MatBolt;
import edu.fudan.stormcv.spout.MatSpout;
import edu.fudan.stormcv.operation.matop.MatGrayOp;
import edu.fudan.stormcv.operation.matop.MatRTMPWriterOp;

public class MatReaderTopology extends BaseTopology {

    private String streamId;
    private String rtmpAddr;
    private String videoAddr;

    private String effect;

    public MatReaderTopology(String streamId, String rtmpAddr, String videoAddr) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        effect = null;
        conf.setNumWorkers(2);
    }

    public MatReaderTopology(String streamId, String rtmpAddr, String videoAddr, String effect) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        this.effect = effect;
        conf.setNumWorkers(3);
    }

    @Override
    public void setSpout() {
        builder.setSpout("matSpout", new MatSpout(videoAddr, streamId));
    }

    @Override
    public void setBolts() {
        String source = "matSpout";
        if (effect != null && !effect.isEmpty()) {
            if (effect.equals("gray")) {
                builder.setBolt("gray", new MatBolt(new MatGrayOp()),
                        4).shuffleGrouping(source);
                source = "gray";
            }
        }
        builder.setBolt(
                "streamer",
                new MatBolt(new MatRTMPWriterOp()
                        .RTMPServer(rtmpAddr).appName(streamId).frameRate(25)),
                1).shuffleGrouping(source);
    }

    @Override
    public String getStreamId() {
        return streamId;
    }
}
