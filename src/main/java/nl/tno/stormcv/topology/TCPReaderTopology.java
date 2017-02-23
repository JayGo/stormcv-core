package nl.tno.stormcv.topology;

import nl.tno.stormcv.bolt.SingleH264InputBolt;
import org.apache.storm.Config;

import nl.tno.stormcv.operation.CannyEdgeOp;
import nl.tno.stormcv.operation.ColorHistogramOp;
import nl.tno.stormcv.operation.FGExtranctionOp;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.SingleRTMPWriterOp;
import nl.tno.stormcv.spout.TCPReaderSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPReaderTopology extends BaseTopology {

	private static final Logger logger = LoggerFactory.getLogger(TCPReaderTopology.class);
	private String streamId;
	private String serverIp;
	private String rtmpAddr;
	private String effect;
	private int port;

	public TCPReaderTopology(String streamId, String rtmpAddr, String serverIp,
			int port) {
		this.streamId = streamId;
		this.serverIp = serverIp;
		this.rtmpAddr = rtmpAddr;
		this.port = port;
		effect = "";
		conf.setNumWorkers(2);
	}

	public TCPReaderTopology(String streamId, String rtmpAddr, String effect,
			String serverIp, int port) {
		this.streamId = streamId;
		this.serverIp = serverIp;
		this.rtmpAddr = rtmpAddr;
		this.port = port;
		this.effect = effect;
		conf.setNumWorkers(6);
	}
	
	public TCPReaderTopology(String streamId, String rtmpAddr, String effect,
			String serverIp, int port, int workerNum) {
		this.streamId = streamId;
		this.serverIp = serverIp;
		this.rtmpAddr = rtmpAddr;
		this.port = port;
		this.effect = effect;
		conf.setNumWorkers(workerNum);
	}

	@Override
	public void setSpout() {
		builder.setSpout("tcpSpout", new TCPReaderSpout(streamId, serverIp,
				port));
	}

	@Override
	public void setBolts() {
		String source = "tcpSpout";
		if (effect != null && !effect.isEmpty()) {
			if (effect.equals("gray")) {
				builder.setBolt("gray", new SingleH264InputBolt(new GrayscaleOp()),
						4).shuffleGrouping(source);
				source = "gray";
			}
			else if (effect.equals("cannyEdge")) {
				builder.setBolt("cannyEdge",
						new SingleH264InputBolt(new CannyEdgeOp()), 4)
						.shuffleGrouping(source);
				source = "cannyEdge";
			}
			else if (effect.equals("colorHistogram")) {
				builder.setBolt("colorHistogram",
						new SingleH264InputBolt(new ColorHistogramOp(streamId)), 4)
						.shuffleGrouping(source);
				source = "colorHistogram";
			}
			else if (effect.equals("foregroundExtraction")) {
				builder.setBolt("foregroundExtraction",
						new SingleH264InputBolt(new FGExtranctionOp()), 1)
						.shuffleGrouping(source);
				source = "foregroundExtraction";
				conf.setNumWorkers(3);
				conf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m -Xms2048m -Xmn2048m -XX:PermSize=1024m -XX:MaxPermSize=2048m -XX:-PrintGCDetails -XX:-PrintGCTimeStamps");
				conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192);
				conf.put(Config.WORKER_HEAP_MEMORY_MB, 4096);
				conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 4096);
				conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 1024);
			}
		}
		logger.info("rtmpAddr: "+rtmpAddr+", streamId: "+streamId);
		builder.setBolt(
				"streamer",
				new SingleH264InputBolt(new SingleRTMPWriterOp()
						.RTMPServer(rtmpAddr).appName(streamId).frameRate(25)),
				1).shuffleGrouping(source);
	}

	@Override
	public String getStreamId() {
		return streamId;
	}

}
