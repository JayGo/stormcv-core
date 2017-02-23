package nl.tno.stormcv.topology;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.spout.CVParticleSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class GrayScaledTopology {

	public static void main(String[] args) {
		StormCVConfig conf = new StormCVConfig();

		conf.setNumWorkers(4);
		conf.setMaxSpoutPending(32);
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);

		List<String> urls = new ArrayList<String>();
		urls.add(GlobalConstants.PseudoRtspAddress);
		int frameSkip = 1;
		Boolean local = true;

		// now create the topology itself
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
				urls).frameSkip(frameSkip)), 1);
		// add bolt that scales frames down to 66% of the original size
		builder.setBolt("scale", new SingleInputBolt(new ScaleImageOp(0.66f)),
				1).shuffleGrouping("spout");
		// add bolt that converts color frames to spout
		builder.setBolt("gray", new SingleInputBolt(new GrayscaleOp()), 1)
				.shuffleGrouping("scale");
		// add bolt that creates a web service on port 8558 enabling users to
		// view the result
		builder.setBolt(
				"streamer",
				new BatchInputBolt(new SlidingWindowBatcher(2, frameSkip)
						.maxSize(6), new MjpegStreamingOp().port(8558).framerate(30))
						.groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
				.shuffleGrouping("gray");

		if (local) {
			try {
				// run in local mode
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("grayscaled", conf,
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
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
