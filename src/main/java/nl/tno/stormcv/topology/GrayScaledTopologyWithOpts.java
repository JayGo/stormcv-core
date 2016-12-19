package nl.tno.stormcv.topology;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.CVParticleBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.ImageFetcher;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.RTMPWriterOp;
import nl.tno.stormcv.spout.CVParticleSpout;
import nl.tno.stormcv.util.OperationUtils;

public class GrayScaledTopologyWithOpts {
	
	private static Logger logger = LoggerFactory.getLogger(GrayScaledTopologyWithOpts.class);

	public static void main(String[] args) throws ParseException {
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		conf.setNumWorkers(4);
		conf.setMaxSpoutPending(32);
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); 
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); 
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); 

		List<String> urls = new ArrayList<String>();
	    //urls.add( "rtsp://streaming3.webcam.nl:1935/n224/n224.stream" );
		// urls.add("rtsp://admin:123456qaz@10.134.141.176:554/h264/ch1/main/av_stream");
	    
	    /*
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
		logger.info(userDir);
		List<String> files = new ArrayList<String>();
		files.add( "file://"+ userDir +"/resources/data/" ); 
		*/

		int frameSkip = 1;
		Boolean local = true;
		Boolean isStream = true;
		List<String> files = null;

		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("cl", "cluster", false, "cluster mode");
		Option operationOption = Option.builder("op")
				.hasArgs()
				.desc("operations")
				.build();
		options.addOption(operationOption);
		Option streamOption = Option.builder("st")
				.hasArgs()
				.numberOfArgs(1)
				.desc("stream name and stream location")
				.build();
		options.addOption(streamOption);
		options.addOption("d", "directory", true, "the directory of image file");

		// parse the command line
		CommandLine commandLine = parser.parse(options, args);
		if (commandLine.hasOption("cl")) {
			local = false;
		}
		String[] operations = {};
		if (commandLine.hasOption("op")) {
			operations = commandLine.getOptionValues("op");
			for (int i = 0; i < operations.length; i++) {
				logger.info("op:" + operations[i]);
			}
		}
		if (commandLine.hasOption("st")) {
			String[] streamPars = commandLine.getOptionValues("st");
			for (int i = 0; i < streamPars.length; i++) {
				logger.info(i+" st:" + streamPars[i]);
				
				urls.add(streamPars[i]);
				
			}
		}
		if (commandLine.hasOption("d")) {
			String dir = commandLine.getOptionValue("d");
			files = new ArrayList<>();
			files.add("file://"+dir);
			isStream = false;
		}
		
		// now create the topology itself 
		TopologyBuilder builder = new TopologyBuilder();
		String preOperation = "spout";
		
		// just one spout reading streams; 
		// i.e. this spout reads two streams in parallel
		if (isStream) {
			builder.setSpout(preOperation, new CVParticleSpout(
					new OpenCVStreamFrameFetcher(urls).frameSkip(frameSkip)), 1);
			builder.setBolt(
					"streamer0",
					new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
							.maxSize(6), 
							new RTMPWriterOp().appName("221").frameRate(30))
							.groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
					.shuffleGrouping(preOperation); 
		} else {
			builder.setSpout(preOperation, new CVParticleSpout(
					new ImageFetcher(files).sleepTime(100) ), 1 );
		}

		
		for (int i = 0; i < operations.length; ++i) {		
			CVParticleBolt bolt = OperationUtils.operationToBolt(operations[i]);
			if (bolt == null) {
				logger.info("cannot initialize the " + operations[i] + " bolt");
				continue;
			}
			if (OperationUtils.isSingleOperation(operations[i])) {
				logger.info("set bolt " + operations[i] + " from " + preOperation);
				builder.setBolt(operations[i], bolt,4).shuffleGrouping(preOperation);
				preOperation = operations[i];
			} else {
				continue;
			}
		}
		
		if (isStream) {
			builder.setBolt(
					"streamer",
					new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
							.maxSize(6), 
							new RTMPWriterOp().appName("221grayscale").frameRate(30))
							.groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
					.shuffleGrouping(preOperation); 
		} else {
			logger.info(files.get(0)+"/output/");
			// simple bolt that draws Features (i.e. locations of features) 
			// into the frame and writes the frame to the local file system at /output/
			builder.setBolt("drawer", new SingleInputBolt(
					new DrawFeaturesOp().destination(files.get(0)+"/output/")), 1)
				.shuffleGrouping(preOperation);
		}
		
		if (local) {
			try {
				// run in local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("grayscaledwithopts", conf,
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
				//Utils.sleep(1200 * 1000); 
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
