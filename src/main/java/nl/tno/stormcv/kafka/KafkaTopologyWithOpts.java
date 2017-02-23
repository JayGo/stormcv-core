package nl.tno.stormcv.kafka;

import java.io.IOException;
import java.util.UUID;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.CVParticleBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.RTMPWriterOp;
import nl.tno.stormcv.constant.ZKConstant;
import nl.tno.stormcv.util.OperationUtils;

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
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.joran.spi.JoranException;

public class KafkaTopologyWithOpts {

    private static Logger logger = LoggerFactory.getLogger(KafkaTopologyWithOpts.class);

    public static void main(String[] args) throws ParseException, IOException, JoranException {
        // LogBackConfigLoader.load("logback.xml");

        StormCVConfig conf = new StormCVConfig();
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(128);
        conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "linux64_opencv_java248.so");
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);
        conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
        conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);
        conf.put(StormCVConfig.FTP_USERNAME, "jkyan");
        conf.put(StormCVConfig.FTP_PASSWORD, "jkyan");

        String topic = "testtopic";
        Boolean local = true;
        int frameSkip = 1;

        // parse the command line
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("cl", "cluster", false, "cluster mode");
        options.addOption("t", "topic", true, "topic");
        options.addOption("op", "operations", true, "operations");

        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("cl")) {
            local = false;
        }

        if (commandLine.hasOption("t")) {
            topic = commandLine.getOptionValue("t");
        }

        String[] operations = {};
        if (commandLine.hasOption("op")) {
            operations = commandLine.getOptionValues("op");
            for (int i = 0; i < operations.length; i++) {
                logger.info("op:" + operations[i]);
                if (operations[i].equals("sift_features") || operations[i].equals("gray") || operations[i].equals("colorhistogram") || operations[i].equals("canny_edge") || operations[i].equals("enhance")) {
                    conf.setNumWorkers(3);
                }
            }
        }

        // for storm-kafka test
        String zks = ZKConstant.ZookeeperServer;
        String zkRoot = "/" + topic; // default zookeeper root configuration for storm
        String id = UUID.randomUUID().toString();
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme());
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConf.ignoreZkOffsets = true;
        //spoutConf.forceFromStart = false;
        spoutConf.zkServers = ZKConstant.ZookeeperServerList;
        spoutConf.zkPort = ZKConstant.ZkPort;
        spoutConf.zkRoot = zkRoot;
        //spoutConf.fetchSizeBytes = 104857600;
        spoutConf.bufferSizeBytes = 104857600;
        spoutConf.stateUpdateIntervalMs = 100;
        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
        //spoutConf.maxOffsetBehind=64;

        TopologyBuilder builder = new TopologyBuilder();
        String preOperation = "spout";

        builder.setSpout(preOperation, new KafkaSpout(spoutConf), 1);


//		builder.setBolt("gray", new SingleH264InputBolt(new GrayscaleOp()), 1)
//			.shuffleGrouping(preOperation);

//		builder.setBolt(
//				"drawer",
//				new SingleH264InputBolt(new DrawFeaturesOp()
//						.destination(destination)), n3)
//						.shuffleGrouping("gray");


//        builder.setBolt(
//				"streamer0",
//				new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
//						.maxSize(6), 
//						new RTMPWriterOp().appName(topic).frameRate(30))
//						.groupBy(new Fields(FrameSerializer.STREAMID)), 1)
//				.shuffleGrouping("spout"); 

        for (int i = 0; i < operations.length; ++i) {
            CVParticleBolt bolt = OperationUtils.operationToBolt(operations[i]);
            if (bolt == null) {
                logger.info("cannot initialize the " + operations[i] + " bolt");
                continue;
            }
            if (OperationUtils.isSingleOperation(operations[i])) {
                logger.info("set bolt " + operations[i] + " from " + preOperation);

                int executor_count = 2;
                if (operations[i].equals("sift_features")) {
                    executor_count = 16;
                }
                if (operations[i].equals("gray") || operations[i].equals("colorhistogram") || operations[i].equals("canny_edge") || operations[i].equals("enhance")) {
                    executor_count = 6;
                }

                builder.setBolt(operations[i], bolt, executor_count).shuffleGrouping(preOperation);

                if (operations[i].equals("sift_features")) {
                    builder.setBolt(operations[i] + "_drawer", new SingleInputBolt(new DrawFeaturesOp()), 4).shuffleGrouping(operations[i]);
                    preOperation = operations[i] + "_drawer";
                } else {
                    preOperation = operations[i];
                }
            } else {
                continue;
            }
        }

//		String userDir = "file://"+System.getProperty("user.dir").replaceAll("\\\\", "/");
//		System.out.println(userDir);
//		builder.setBolt("drawer", new SingleH264InputBolt(
//				new DrawFeaturesOp().destination(userDir+"/output/")), 1)
//			.shuffleGrouping(preOperation);

        builder.setBolt(
                "streamer",
                new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
                        .maxSize(6),
                        new RTMPWriterOp().appName(topic + "-opts").frameRate(30))
                        .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                .shuffleGrouping(preOperation);

        String topoName = "kafka-with-opts-" + topic;
        if (local) {
            try {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topoName, conf,
                        builder.createTopology());
                Utils.sleep(3600 * 1000);
                cluster.shutdown();
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar(topoName,
                        conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
