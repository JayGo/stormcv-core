package nl.tno.stormcv.topology;

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
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.PNG_IMAGE);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
        conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
        conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);

        // List<String> urls = new ArrayList<String>();
        // urls.add("rtsp://streaming3.webcam.nl:1935/n224/n224.stream");
        // urls.add("rtsp://streaming3.webcam.nl:1935/n233/n233.stream");
        // urls.add("");
        // urls.add("rtsp://admin:12345@10.134.141.155:554/h264/ch1/main/av_stream");

        int frameSkip = 1;
        Boolean local = true;
        String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
        List<String> files = new ArrayList<String>();
        files.add( "file://"+ userDir +"/resources/data/" );

        // now create the topology itself
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new CVParticleSpout(
                new ImageFetcher(files).sleepTime(100) ), 8);


        // add bolt that converts color frames to spout
        builder.setBolt("inpaint", new SingleInputBolt(new InpaintOp()), 8)
                .shuffleGrouping("spout");

        builder.setBolt("drawer", new SingleInputBolt(
                new DrawFeaturesOp().destination(files.get(0)+"/output/")), 8)
                .shuffleGrouping("inpaint");

        if (local) {
            try {
                // run in local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("inpaint", conf,
                        builder.createTopology());
                // run for one minute and then kill the topology
                Utils.sleep(30000 * 1000);
                cluster.shutdown();
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar("grayscaled",
                        conf, builder.createTopology());
                Utils.sleep(1200 * 1000);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
