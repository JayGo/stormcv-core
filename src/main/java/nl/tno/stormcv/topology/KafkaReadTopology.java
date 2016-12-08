package nl.tno.stormcv.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.CollectorBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.SingleRTMPWriterOp;
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

@SuppressWarnings("unused")
public class KafkaReadTopology {
	
	public static void main(String[] args) throws ParseException, IOException {
		// LogBackConfigLoader.load("logback.xml");
		
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(128);
		conf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m -Xms4096m -Xmn4096m");
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "linux64_opencv_java248.so");
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); 
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); 
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); 
		conf.put(StormCVConfig.FTP_USERNAME, "jkyan");
		conf.put(StormCVConfig.FTP_PASSWORD, "jkyan");
		
		String topic= "testtopic";
		Boolean local = true;
		int frameSkip =1;
		
		List<String> urls = new ArrayList<String>();
		String address;
		
		// urls.add("rtsp://218.204.223.237:554/live/1/66251FC11353191F/e7ooqwcfbqjoo80j.sdp");
		

		// parse the command line
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
			topic = address;
			address = "rtsp://admin:123456qaz@10.134.141." + address + ":554/h264/ch1/main/av_stream";
			urls.add(address);
		}
		
		//NativeUtils.loadOpencvLib();
		
		 //for storm-kafka test
//		String zks = "10.134.142.115:2181,10.134.142.116:2181,10.134.142.119:2181";
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
//        spoutConf.zkServers = Arrays.asList(new String[] {"10.134.142.115","10.134.142.116","10.134.142.119"});
//        //spoutConf.zkServers = Arrays.asList(new String[] {"10.134.143.51"});
//        spoutConf.zkPort = 2181;
//        spoutConf.zkRoot = "/" + topic;
//        //spoutConf.fetchSizeBytes = 104857600;
//        //spoutConf.zkRoot = "";
//        spoutConf.bufferSizeBytes = 104857600;
//        //spoutConf.stateUpdateIntervalMs = 2000;
//        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
//        spoutConf.maxOffsetBehind=64;
//       
//        //create the topology
  		TopologyBuilder builder = new TopologyBuilder();
  		String preOperation = "spout";
//  		//KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
//  		
//  		// create kafka-spout
        //builder.setSpout(preOperation, new KafkaSpout(spoutConf), 1);
        
        //List<String> urls = new ArrayList<String>();
		//urls.add("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
        builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
				urls).frameSkip(frameSkip)), 1);
	
        //add bolt to transfer
//		builder.setBolt("transfer", new CollectorBolt(), 1)
//			.shuffleGrouping("spout");
        
		//add bolt that converts color frames to spout 
//		builder.setBolt("gray", new SingleInputBolt(new GrayscaleOp()), 16)
//			.shuffleGrouping("spout");
		
//		builder.setBolt("colloctor", new CollectorBolt(), 1)
//			.shuffleGrouping("gray");

		// simple bolt that draws Features (i.e. locations of features) into the frame and writes the frame to the local file system at /output/facedetections
//		builder.setBolt(
//				"drawer",
//				new SingleInputBolt(new DrawFeaturesOp()
//						.destination(destination)), n3)
//						.shuffleGrouping("gray");
//        builder.setBolt(
//				"streamer0",
//				new SingleInputBolt(new SingleRTMPWriterOp().appName(topic).frameRate(25)), 4)
//				.shuffleGrouping("spout");
        
        
		/**
		 * this bolt using MjpegStreamingOp for video show in web
		 **/
        builder.setBolt(
				"streamer",
				new BatchInputBolt(new SlidingWindowBatcher(1, frameSkip)
						.maxSize(6), // note the required batcher used as a
									 // buffer and maintains the order of the
									 // frames
						new MjpegStreamingOp().port(8558).framerate(1000))
						.groupBy(new Fields(FrameSerializer.STREAMID)), 1)
				.shuffleGrouping("spout");
		
		if (local) {
			try {
				// run in local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("kafka-raw-"+topic, conf,
						builder.createTopology());
				// run for one minute and then kill the topology
				Utils.sleep(360000 * 1000);
				cluster.shutdown();
				System.exit(1);   
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				StormSubmitter.submitTopologyWithProgressBar("kafka-raw-"+topic,
						conf, builder.createTopology());
				//Utils.sleep(30 * 1000); 
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
