package nl.tno.stormcv.topology;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.util.RunCmdUtil;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public abstract class BaseTopology {

	protected TopologyBuilder builder = new TopologyBuilder();
	protected StormCVConfig conf;
	private LocalCluster cluster;

	private boolean isTopologyRunningAtLocal;

	public BaseTopology() {
		conf = new StormCVConfig();
		// conf.setNumWorkers(1);
		conf.setMaxSpoutPending(64);
		conf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m -Xms4096m -Xmn2048m -XX:PermSize=1024m -XX:MaxPermSize=2048m -XX:MaxDirectMemorySize=512m");
		conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192);
		conf.put(Config.WORKER_HEAP_MEMORY_MB, 4096);
		conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 4096);
		//conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 1024);

		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "linux64_opencv_java248.so");
//		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false);
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30);
		conf.put(StormCVConfig.FTP_USERNAME, "jkyan");
		conf.put(StormCVConfig.FTP_PASSWORD, "jkyan");

		isTopologyRunningAtLocal = false;
	}

	public abstract void setSpout();

	public abstract void setBolts();

	public abstract String getStreamId();

	public void submitTopologyToLocal() {
		setSpout();
		setBolts();
		cluster = new LocalCluster();
		cluster.submitTopology(getStreamId(), conf, builder.createTopology());
		isTopologyRunningAtLocal = true;
	}

	public void submitTopologyToCluster() throws Exception {
		setSpout();
		setBolts();
		StormSubmitter.submitTopologyWithProgressBar(getStreamId(), conf,
				builder.createTopology());
	}

	public String killTopology() {
		try {
			if (isTopologyRunningAtLocal) {
				cluster.killTopology(getStreamId());
				cluster.shutdown();
			}
			else {
				// code killing style
				// if(conf.containsKey("nimbus.host")) {
				// conf.remove("nimbus.host");
				// }
				// if(!conf.containsKey("nimbus.seeds")) {
				// List<String> nimbusSeeds = new ArrayList<String>();
				// nimbusSeeds.add("10.134.142.100");
				// conf.put("nimbus.seeds", nimbusSeeds);
				// }
				// Nimbus.Client client = NimbusClient.getConfiguredClient(conf)
				// .getClient();
				// List<TopologySummary> topologyList = client.getClusterInfo()
				// .get_topologies();
				// for (TopologySummary topologySummary : topologyList) {
				// if (topologySummary.get_name().equals(getStreamId())) {
				// KillOptions killOpts = new KillOptions();
				// killOpts.set_wait_secs(5);
				// client.killTopologyWithOpts(getStreamId(), killOpts);
				// System.out.println("killed " + getStreamId());
				// return "Succeed to kill " + getStreamId();
				// }
				// }

				// command killing style
				String killCmd = "/usr/local/storm/bin/storm kill "
						+ getStreamId();
				if (RunCmdUtil.doWaitFor(killCmd) == 0) {
					return "Succeed";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "Error, failed to kill topology " + getStreamId();
	}

}
