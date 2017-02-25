package nl.tno.stormcv.topology;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.ImageFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.operation.*;
import nl.tno.stormcv.spout.CVParticleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

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
        files.add( "file://"+ userDir +"/resources/data/" );

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new CVParticleSpout(
                new ImageFetcher(files).sleepTime(100) ), 8);

        builder.setBolt("inpaint", new SingleInputBolt(new InpaintOp()), 8)
                .shuffleGrouping("spout");
        String outDir = files.get(0)+"/output/";
        File tmp = new File(outDir);
        if (!tmp.exists()) {
            tmp.mkdir();
        }
        builder.setBolt("drawer", new SingleInputBolt(
                new DrawFeaturesOp().destination(outDir)), 8)
                .shuffleGrouping("inpaint");

        String topoName = "InpaintTopo";
        if (local) {
            try {
                // run in local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topoName, conf,
                        builder.createTopology());
                // run for one minute and then kill the topology
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
