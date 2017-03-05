package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.bolt.StatCollectorBolt;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.fetcher.ImageUrlFetcher;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.operation.single.ImageFetchAndOperation;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.spout.CVParticleSignalSpout;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:24 AM
 * Description:
 */
public class ImageFetchTopologyJPEG extends BaseTopology {

    private String streamId = "ImageFetchTopologyJPEG";
    private String readLocations = "file:///home/nfs/images/480p";
    private String writeLocations = "file:///home/nfs/images/480p/output";

    public static void main(String[] args) {
        ImageFetchTopologyJPEG topology = new ImageFetchTopologyJPEG();
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ImageFetchTopologyJPEG() {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        isTopologyRunningAtLocal = true;
    }

    @Override
    public void setSpout() {
        builder.setSpout("spout", new CVParticleSignalSpout("request", new ImageUrlFetcher("cat", readLocations, writeLocations,
                BOLT_OPERTION_TYPE.COLORHISTOGRAM).batchSize(32)), 1);
    }

    @Override
    public void setBolts() {
        builder.setBolt("operation", new SingleJPEGInputBolt(new ImageFetchAndOperation(),
                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("spout");

        builder.setBolt("collector", new StatCollectorBolt(), 1).localOrShuffleGrouping("operation");
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }
}
