package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.bolt.StatCollectorBolt;
import edu.fudan.stormcv.constant.BoltHandleType;
import edu.fudan.stormcv.constant.BoltOperationType;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.fetcher.ImageUrlFetcher;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.operation.single.ImageFetchAndOperation;
import edu.fudan.stormcv.operation.single.InpaintOp;
import edu.fudan.stormcv.spout.CVParticleSignalSpout;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/6/17 - 7:06 AM
 * Description:
 */
public class ImageInpaintingTopology extends BaseTopology {

    private String streamId = "image_inpaint";
    private String readLocations = "file:///home/nfs/images/inpaint";
    private String writeLocations = "file:///home/nfs/images/inpaint/output";

    public static void main(String[] args) {
        ImageInpaintingTopology topology = new ImageInpaintingTopology();
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ImageInpaintingTopology() {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.PNG_IMAGE);
        isTopologyRunningAtLocal = true;
    }

    @Override
    public void setSpout() {
        builder.setSpout("spout", new CVParticleSignalSpout(GlobalConstants.ImageRequestZKRoot,
                new ImageUrlFetcher(streamId, readLocations, writeLocations,
                BoltOperationType.CUSTOM).batchSize(32)), 1);
    }

    @Override
    public void setBolts() {
        builder.setBolt("operation", new SingleJPEGInputBolt(new ImageFetchAndOperation(new InpaintOp(),
                BoltHandleType.BOLT_HANDLE_TYPE_BUFFEREDIMAGE),
                BoltHandleType.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1).localOrShuffleGrouping("spout");

        builder.setBolt("collector", new StatCollectorBolt(), 1).localOrShuffleGrouping("operation");
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }
}
