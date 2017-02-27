package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.fetcher.ImageFetcher;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.operation.single.DrawFeaturesOp;
import edu.fudan.stormcv.operation.single.InpaintOp;
import edu.fudan.stormcv.spout.CVParticleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class InpaintTopology {
    public static void main(String[] args) throws InvalidTopologyException {
        StormCVConfig conf = new StormCVConfig();
        conf.setNumWorkers(4);
        conf.setMaxSpoutPending(32);
//        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.PNG_IMAGE);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
        conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
        conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);

        Boolean local = true;
        String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
        List<String> files = new ArrayList<String>();
        files.add("file://" + userDir + "/resources/data/");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new CVParticleSpout(
                new ImageFetcher(files).sleepTime(100)), 8);

        builder.setBolt("inpaint", new SingleJPEGInputBolt(new InpaintOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 8)
                .shuffleGrouping("spout");
        String outDir = files.get(0) + "/output/";
        File tmp = new File(outDir);
        if (!tmp.exists()) {
            tmp.mkdir();
        }
        builder.setBolt("drawer", new SingleJPEGInputBolt(
                new DrawFeaturesOp().destination(outDir), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 8)
                .shuffleGrouping("inpaint");

        String topoName = "InpaintTopo";
        if (local) {
            try {
                // run in local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topoName, conf,
                        builder.createTopology());
                Utils.sleep(3000 * 1000);
                cluster.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar(topoName,
                        conf, builder.createTopology());
            } catch (AlreadyAliveException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
