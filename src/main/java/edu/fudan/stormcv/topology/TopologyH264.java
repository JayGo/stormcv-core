package edu.fudan.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.jliu.message.BaseMessage;
import edu.fudan.jliu.message.EffectMessage;
import edu.fudan.lwang.codec.SourceInfo;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchH264InputBolt;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.constant.BoltOperationType;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.spout.TCPCaptureSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TopologyH264 extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(TopologyH264.class);
    private String streamId;
    private String videoAddr;
    private String rtmpAddr = GlobalConstants.DefaultRTMPServer;
    private SourceInfo sourceInfo;
    private int frameSkip = 0;
    private float frameRate = 25.0f;
    private BoltOperationType type;

    private boolean sendRtmp = true;

    private boolean isEffect = false;

    private Map<String, Object> effectParams;
    
    public static void main(String args[]) {
    	LibLoader.loadHgCodecLib();
        LibLoader.loadOpenCVLib();
        LibLoader.loadRtmpStreamerLib();
    	TopologyH264 mTopologyH264 = new TopologyH264("test1", GlobalConstants.DefaultRTMPServer, GlobalConstants.Pseudo720pFaceRtspAddress, "gray");
    	try {
			mTopologyH264.submitTopology();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public TopologyH264(String streamId, String rtmpAddr, String videoAddr) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        this.type = BoltOperationType.UNSUPPORT;
        isTopologyRunningAtLocal = true;
        conf.setNumWorkers(2);
        this.effectParams = new HashMap<>();
    }
    
    public TopologyH264(String streamId, String rtmpAddr, String videoAddr, String effectType) {
        this(streamId,rtmpAddr,videoAddr);
    	this.type = findEffectType(effectType);
    }
    
    public TopologyH264(BaseMessage msg) {
		// TODO Auto-generated constructor stub
        this.streamId = msg.getStreamId();
        this.rtmpAddr = msg.getRtmpAddr();
        this.videoAddr = msg.getAddr();
        this.type = BoltOperationType.UNSUPPORT;
        isTopologyRunningAtLocal = true;
    	if(msg instanceof EffectMessage) {
    		isEffect = true;
    		EffectMessage emsg = (EffectMessage) msg;
    		this.type = findEffectType(emsg.getEffectType());
    	}
    	conf.setNumWorkers(2);

        this.effectParams = new HashMap<>();
	}

	public TopologyH264(String streamId, String rtmpAddr, String videoAddr, String effectType, Map<String, Object> effectParams, float frameRate) {
        this(streamId, rtmpAddr, videoAddr, effectType);
        if (effectParams != null) {
            this.effectParams.putAll(effectParams);
        }
        this.frameRate = frameRate;
    }

    public TopologyH264 isTopologyRunningAtLocal(boolean isTopologyRunningAtLocal) {
        this.isTopologyRunningAtLocal = isTopologyRunningAtLocal;
        return this;
    }

    public boolean isTopologyRunningAtLocal() {
        return isTopologyRunningAtLocal;
    }

    public Config getConfig() {
        Config config = new Config();
        config.putAll(this.conf);
        config.putAll(Utils.readCommandLineOpts());
        config.putAll(Utils.readStormConfig());
        return config;
    }
    
    private BoltOperationType findEffectType(String effect) {
    	if(effect.equals(BoltOperationType.GRAY.toString())) {
    		return BoltOperationType.GRAY;
    	} else if(effect.equals(BoltOperationType.COLORHISTOGRAM.toString())) {
    		return BoltOperationType.COLORHISTOGRAM;
    	} else if(effect.equals(BoltOperationType.FACEDETECT.toString())){
    		return BoltOperationType.FACEDETECT;
    	} else {
    		return BoltOperationType.UNSUPPORT;
    	}
    }

    @Override
    public void setSpout() {
        TCPCaptureSpout tcpCaptureSpout = new TCPCaptureSpout(streamId, videoAddr, CodecType.CODEC_TYPE_H264_CPU);
        builder.setSpout("tcpSpout", tcpCaptureSpout);
        sourceInfo = tcpCaptureSpout.getSourceInfo();
        if (sourceInfo == null) {
            logger.info("sourceInfo returned is null! System exit!");
            System.exit(-1);
        }
    }

    @Override
    public void setBolts() {
        createOperationBolt(type, "tcpSpout");
        if (!sendRtmp) {
            createOperationBolt(BoltOperationType.MJPEGSTREAMER, type.toString());
        } else {
            createOperationBolt(BoltOperationType.RTMPSTREAMER, type.toString());
        }
    }

    public void createOperationBolt(BoltOperationType type, String sourceComp) {
        switch (type) {
            case GRAY: {
                builder.setBolt(BoltOperationType.GRAY.toString(),
                        new SingleH264InputBolt(new GrayImageOp()).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            /*scale not supported*/
//            case SCALE: {
//                builder.setBolt(BoltOperationType.SCALE.toString(),
//                        new SingleH264InputBolt(new ScaleImageOp(0.5f).useMat(true)).setSourceInfo(sourceInfo), 1)
//                        .localOrShuffleGrouping(sourceComp);
//                break;
//            }
            case COLORHISTOGRAM: {
                builder.setBolt(BoltOperationType.COLORHISTOGRAM.toString(),
                        new SingleH264InputBolt(new ColorHistogramOp(streamId)
                                .useMat(true).outputFrame(true)).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case FACEDETECT: {
                builder.setBolt(BoltOperationType.FACEDETECT.toString(),
                        new SingleH264InputBolt(new HaarCascadeOp(streamId, GlobalConstants.HaarCacascadeXMLFileName)
                                .useMat(true).outputFrame(true)).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case MJPEGSTREAMER: {
                builder.setBolt(BoltOperationType.MJPEGSTREAMER.toString(),
                        new BatchH264InputBolt(new SlidingWindowBatcher(2, frameSkip)
                                .maxSize(6), new MjpegStreamingOp()
                                .useMat(true).port(8558).framerate((int)frameRate)).setSourceInfo(sourceInfo)
                                .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case RTMPSTREAMER: {
                builder.setBolt(BoltOperationType.RTMPSTREAMER.toString(),
                        new SingleH264InputBolt(new H264RtmpStreamOp().RTMPServer(rtmpAddr).appName(streamId)
                                .frameRate(frameRate)).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            default: {
                break;
            }
        }
    }

    @Override
    public String getStreamId() {
        return streamId;
    }


}
