package nl.tno.stormcv.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.operation.CannyEdgeOp;
import nl.tno.stormcv.operation.CodecTestOperation;
import nl.tno.stormcv.operation.ColorHistogramOp;
import nl.tno.stormcv.operation.EmptyOperation;
import nl.tno.stormcv.operation.EmptyOperation2;
import nl.tno.stormcv.operation.FGExtranctionOp;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.SingleRTMPWriterOp;
import nl.tno.stormcv.spout.TCPCaptureSpout;

public class TCPCaptureTopology extends BaseTopology {
	


	private static final Logger logger = Logger.getLogger(TCPCaptureTopology.class);
	private String streamId;
	private String videoAddr;
	private String rtmpAddr;
	private String effect;
	private SourceInfo sourceInfo;
	
	public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr) {
		this.streamId = streamId;
		this.rtmpAddr = rtmpAddr;
		this.videoAddr = videoAddr;
		effect = "";
		conf.setNumWorkers(2);
	}
	
	public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr, String effect) {
		this.streamId = streamId;
		this.videoAddr = videoAddr;
		this.rtmpAddr = rtmpAddr;
		this.effect = effect;
		conf.setNumWorkers(6);
	}
	
	public TCPCaptureTopology(String streamId, String rtmpAddr, String videoAddr, String effect, int workerNum) {
		this(streamId, rtmpAddr, videoAddr, effect);
		conf.setNumWorkers(workerNum);
	}

	@Override
	public void setSpout() {
		// TODO Auto-generated method stub
		TCPCaptureSpout tcpCaptureSpout = new TCPCaptureSpout(streamId, videoAddr, CodecType.CODEC_TYPE_H264_CPU);
		builder.setSpout("tcpSpout", tcpCaptureSpout);
		sourceInfo = tcpCaptureSpout.getSourceInfo();
		if(sourceInfo == null) {
			logger.info("sourceInfo returned is null! System exit!");
			System.exit(-1);
		}
	}

	@Override
	public void setBolts() {
		// TODO Auto-generated method stub
		String source = "tcpSpout";
		if (effect != null && !effect.isEmpty()) {
			if (effect.equals("gray")) {
				builder.setBolt("gray", new SingleInputBolt(new GrayscaleOp()),
						4).shuffleGrouping(source);
				source = "gray";
			}
			else if (effect.equals("cannyEdge")) {
				builder.setBolt("cannyEdge",
						new SingleInputBolt(new CannyEdgeOp()), 4)
						.shuffleGrouping(source);
				source = "cannyEdge";
			}
			else if (effect.equals("colorHistogram")) {
				builder.setBolt("colorHistogram",
						new SingleInputBolt(new ColorHistogramOp(streamId)), 4)
						.shuffleGrouping(source);
				source = "colorHistogram";
			}
			else if (effect.equals("foregroundExtraction")) {
				builder.setBolt("foregroundExtraction",
						new SingleInputBolt(new FGExtranctionOp()), 1)
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
		
		// For codec test
//		builder.setBolt("codec", new SingleInputBolt(new CodecTestOperation()).setSourceInfo(sourceInfo),
//				1).shuffleGrouping(source);
//		source = "codec";
		
		// For debug
		builder.setBolt("debug", new SingleInputBolt(new EmptyOperation()).setSourceInfo(sourceInfo),
				1).shuffleGrouping(source);
		source = "debug";
		
		// For debug
		builder.setBolt("debug2", new SingleInputBolt(new EmptyOperation2()).setSourceInfo(sourceInfo), 
				1).shuffleGrouping(source);
		
		
		// for RTMP operation 
//		builder.setBolt(
//				"streamer",
//				new SingleInputBolt(new SingleRTMPWriterOp()
//						.RTMPServer(rtmpAddr).appName(streamId).frameRate(25)),
//				1).shuffleGrouping(source);
	}

	@Override
	public String getStreamId() {
		// TODO Auto-generated method stub
		return streamId;
	}

}
