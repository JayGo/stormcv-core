package nl.tno.stormcv.topology;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import nl.tno.stormcv.LogBackConfigLoader;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.kafka.MessageScheme;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.GrayscaleOp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import ch.qos.logback.core.joran.spi.JoranException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

@SuppressWarnings("unused")
public class KafkaTestTopology {
	
	public static void main(String[] args) throws ParseException, IOException, JoranException {
		LogBackConfigLoader.load("logback.xml");
		
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		conf.setNumWorkers(6);
		conf.setMaxSpoutPending(128);
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "linux64_opencv_java248.so");
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); 
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); 
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); 
		conf.put(StormCVConfig.FTP_USERNAME, "jkyan");
		conf.put(StormCVConfig.FTP_PASSWORD, "jkyan");
		
		String destination = "ftp://10.134.143.51/";
		int n1 = 1;
		int n2 = 1;
		int n3 = 1;
		String topic= "testtopic";
		Boolean local = true;

		// parse the command line
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("cl", "cluster", false, "cluster mode");
		options.addOption("d", "dir", true, "dir");
		options.addOption("n1", "n1", true, "n1");
		options.addOption("n2", "n2", true, "n2");
		options.addOption("n3", "n3", true, "n3");
		options.addOption("t", "topic", true, "topic");
		CommandLine commandLine = parser.parse(options, args);
		if (commandLine.hasOption("cl")) {
			local = false;
		}
		if (commandLine.hasOption("d")) {
			String dir = commandLine.getOptionValue("d");
			destination += dir;
		} else {
			destination += "test";
		}
		if (commandLine.hasOption("n1")) {
			n1 = Integer.parseInt(commandLine.getOptionValue("n1"));
		}
		if (commandLine.hasOption("n2")) {
			n2 = Integer.parseInt(commandLine.getOptionValue("n2"));
		}
		if (commandLine.hasOption("n3")) {
			n3 = Integer.parseInt(commandLine.getOptionValue("n3"));
		}
		if (commandLine.hasOption("t")) {
			topic = commandLine.getOptionValue("t");
		}
		
		// for storm-kafka test
		String zks = "10.134.143.51:2181,10.134.143.55:2181,10.134.142.111:2181";
        String zkRoot = "/" + topic; // default zookeeper root configuration for storm
        //String zkRoot = ""; // default zookeeper root configuration for storm
		String id = UUID.randomUUID().toString();
       
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme());
        //spoutConf.forceFromStart = false;
		spoutConf.ignoreZkOffsets = false;
        spoutConf.zkServers = Arrays.asList(new String[] {"10.134.143.51","10.134.143.55","10.134.142.111"});
        spoutConf.zkPort = 2181;
        spoutConf.zkRoot = "/" + topic;
        //spoutConf.fetchSizeBytes = 104857600;
        //spoutConf.zkRoot = "";
        spoutConf.bufferSizeBytes = 104857600;
        spoutConf.stateUpdateIntervalMs = 100;
        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
        //spoutConf.maxOffsetBehind=64;
       
        //create the topology
  		TopologyBuilder builder = new TopologyBuilder();
  		String preOperation = "spout";
  		//KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
  		
  		// create kafka-spout
        builder.setSpout(preOperation, new KafkaSpout(spoutConf), n1);
	
		//add bolt that converts color frames to spout 
		builder.setBolt("gray", new SingleInputBolt(new GrayscaleOp()), n2)
			.shuffleGrouping(preOperation);
		
		// simple bolt that draws Features (i.e. locations of features) into the frame and writes the frame to the local file system at /output/facedetections
		builder.setBolt(
				"drawer",
				new SingleInputBolt(new DrawFeaturesOp()
						.destination(destination)), n3)
						.shuffleGrouping("gray");
		
		if (local) {
			try {
				// run in local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("kafka-gray-test", conf,
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
				StormSubmitter.submitTopologyWithProgressBar("kafka-gray-test",
						conf, builder.createTopology());
				Utils.sleep(30 * 1000); 
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
