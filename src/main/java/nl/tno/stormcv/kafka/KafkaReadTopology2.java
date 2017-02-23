package nl.tno.stormcv.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleH264InputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher2;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.spout.CVParticleSpout;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class KafkaReadTopology2 {

    public static void main(String[] args) throws ParseException, IOException {

        StormCVConfig conf = new StormCVConfig();
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(128);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);
        conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
        conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);
        conf.put(StormCVConfig.FTP_USERNAME, "jkyan");
        conf.put(StormCVConfig.FTP_PASSWORD, "jkyan");

        String topic = "testtopic";
        Boolean local = true;
        int frameSkip = 5;

        List<String> urls = new ArrayList<String>();
        String address;
        urls.add(GlobalConstants.PseudoRtspAddress);

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("cl", "cluster", false, "cluster mode");
        options.addOption("t", "topic", true, "topic");
        options.addOption("s", "stream", true, "stream address");
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("cl")) {
            local = false;
        }

        if (commandLine.hasOption("t")) {
            topic = commandLine.getOptionValue("t");
        }

        if (commandLine.hasOption("s")) {
            address = commandLine.getOptionValue("s");
            address = "rtsp://admin:123456qaz@10.134.141." + address + ":554/h264/ch1/main/av_stream";
            urls.add(address);
        }

        /**
         * Configuration of kafka spout
         **/
        //TODO: move the zookeeper info to utils.Constans or config file

//		String zks = ZKConstant.ZookeeperServer;
//		//String zks = "10.134.143.51:2191";
//        String zkRoot = "/" + topic; // default zookeeper root configuration for storm
//        //String zkRoot = ""; // default zookeeper root configuration for storm
//		String id = UUID.randomUUID().toString();
//       
//        BrokerHosts brokerHosts = new ZkHosts(zks);
//        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
//        spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme());
//        //spoutConf.forceFromStart = false;
//		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//		spoutConf.ignoreZkOffsets = true;
//		
//        spoutConf.zkServers = ZKConstant.ZookeeperServerList;
//        //spoutConf.zkServers = Arrays.asList(new String[] {"10.134.143.51"});
//        spoutConf.zkPort = ZKConstant.ZkPort;
//        spoutConf.zkRoot = "/" + topic;
//        //spoutConf.fetchSizeBytes = 104857600;
//        //spoutConf.zkRoot = "";
//        spoutConf.bufferSizeBytes = 104857600;
//        //spoutConf.stateUpdateIntervalMs = 2000;
//        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
//        spoutConf.maxOffsetBehind=64;


        TopologyBuilder builder = new TopologyBuilder();
        String preOperation = "spout";

        /**
         * this spout using KafkaSpout to fetch the video image topic
         **/
//        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
//		builder.setSpout(preOperation, new KafkaSpout(spoutConf), 1);

        /**
         * this spout using OpenCVStreamFrameFetcher to fetch the video image
         **/
        builder.setSpout(preOperation, new CVParticleSpout(
                new OpenCVStreamFrameFetcher2(urls).frameSkip(frameSkip)), 1);

//		builder.setSpout(preOperation, new MatImageSpout(
//				(new OpenCVStreamFrameFetcher2(urls)).frameSkip(frameSkip)), 1);

//		builder.setBolt("preprocess", new MatImageConvertBolt(), 8).shuffleGrouping(preOperation);

        /**
         * this bolt using ScaleImageOp to scale the video image
         **/
        builder.setBolt("scale", new SingleInputBolt(new ScaleImageOp((float) 0.5)), 4).shuffleGrouping("preprocess");

        /**
         * this bolt using MjpegStreamingOp for video show in web
         **/
        builder.setBolt(
                "streamer",
                new BatchInputBolt(new SlidingWindowBatcher(5, frameSkip)
                        .maxSize(6), // note the required batcher used as a
                        // buffer and maintains the order of the
                        // frames
                        new MjpegStreamingOp().port(8558).framerate(30))
                        .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                .shuffleGrouping("scale");

        /**
         * this bolt using DrawFeaturesOp for video image writing to ftp
         **/
//      String destination = "ftp://10.134.143.51/test";
//		builder.setBolt(
//		"drawer",
//		new SingleH264InputBolt(new DrawFeaturesOp()
//				.destination(destination)), 2)
//				.shuffleGrouping("scale");


        /**
         * this bolt using SingleRTMPWriterOp for video image transmitting to RTMP server
         **/
//        builder.setBolt(
//				"streamer0",
//				new SingleH264InputBolt(new SingleRTMPWriterOp().appName(topic).frameRate(25)), 1)
//				.shuffleGrouping("scale");

        /**
         * this bolt using RTMPWriterOp for video image batches transmitting to RTMP server 
         **/
//        builder.setBolt(
//				"streamer0",
//				new BatchInputBolt(new SlidingWindowBatcher(10, frameSkip)
//						.maxSize(10),
//						new RTMPWriterOp().appName(topic).frameRate(30))
//						.groupBy(new Fields(FrameSerializer.STREAMID)), 1)
//				.shuffleGrouping("scale");


        if (local) {
            try {
                // run in local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("kafka-raw-" + topic, conf,
                        builder.createTopology());
                // run for one minute and then kill the topology
                Utils.sleep(1000 * 1000);
                cluster.shutdown();
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar("kafka-raw-" + topic,
                        conf, builder.createTopology());
                Utils.sleep(10 * 1000);
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
