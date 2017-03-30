package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchJPEGInputBolt;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.fetcher.OpenCVStreamFrameFetcher;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.spout.CVParticleSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:24 AM
 * Description:
 */
public class GrayScaleTopologyJPEG extends BaseTopology {

    private int frameSkip = 0;
    private List<String> urls;
    private String streamId = "GrayScaleTopologyJPEG";
    private BOLT_OPERTION_TYPE type;

    private boolean view720p = false;
    private boolean sendRtmp = false;
    
    private String rtmpAddr;
    private String videoAddr;

    public static void main(String[] args) {
        GrayScaleTopologyJPEG topology = new GrayScaleTopologyJPEG("test1", GlobalConstants.DefaultRTMPServer,GlobalConstants.Video720p,"gray");
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public GrayScaleTopologyJPEG(String streamId, String rtmpAddr, String videoAddr, String effectType) {
    	this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        isTopologyRunningAtLocal = true;
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
    	this.type = findEffectType(effectType);
        urls = new ArrayList<String>();
        urls.add(videoAddr);
        sendRtmp=true;
    }
    
    private BOLT_OPERTION_TYPE findEffectType(String effect) {
    	if(effect.equals(BOLT_OPERTION_TYPE.GRAY.toString())) {
    		return BOLT_OPERTION_TYPE.GRAY;
    	} else if(effect.equals(BOLT_OPERTION_TYPE.COLORHISTOGRAM.toString())) {
    		return BOLT_OPERTION_TYPE.COLORHISTOGRAM;
    	} else if(effect.equals(BOLT_OPERTION_TYPE.FACEDETECT.toString())){
    		return BOLT_OPERTION_TYPE.FACEDETECT;
    	} else {
    		return BOLT_OPERTION_TYPE.UNSUPPORT;
    	}
    }

    public GrayScaleTopologyJPEG(BOLT_OPERTION_TYPE type) {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        urls = new ArrayList<String>();
        this.type = type;
        if (view720p) {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
                urls.add(GlobalConstants.Video720p);
            } else {
                urls.add(GlobalConstants.Pseudo720pRtspAddress);
            }
        } else {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
                urls.add(GlobalConstants.Video480p);
            } else {
                urls.add(GlobalConstants.PseudoRtspAddress);
            }
        }
        isTopologyRunningAtLocal = true;
    }

    @Override
    public void setSpout() {
        builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
                urls).frameSkip(frameSkip)), 1);
    }

    @Override
    public void setBolts() {
        createOperationBolt(type, "spout");
        if (!sendRtmp) {
            createOperationBolt(BOLT_OPERTION_TYPE.MJPEGSTREAMER, type.toString());
        } else {
            createOperationBolt(BOLT_OPERTION_TYPE.RTMPSTREAMER, type.toString());
        }
    }

    public void createOperationBolt(BOLT_OPERTION_TYPE type, String sourceComp) {
        switch (type) {
            case GRAY: {
                builder.setBolt(BOLT_OPERTION_TYPE.GRAY.toString(),
                        new SingleJPEGInputBolt(new GrayImageOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case SCALE: {
                builder.setBolt(BOLT_OPERTION_TYPE.SCALE.toString(),
                        new SingleJPEGInputBolt(new ScaleImageOp(0.5f).useMat(false),
                                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case COLORHISTOGRAM: {
                builder.setBolt(BOLT_OPERTION_TYPE.COLORHISTOGRAM.toString(),
                        new SingleJPEGInputBolt(new ColorHistogramOp(streamId).useMat(false).outputFrame(true),
                        BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case FACEDETECT: {
                builder.setBolt(BOLT_OPERTION_TYPE.FACEDETECT.toString(),
                        new SingleJPEGInputBolt(new HaarCascadeOp(streamId, GlobalConstants.HaarCacascadeXMLFileName)
                                .useMat(true).outputFrame(true),
                                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case MJPEGSTREAMER: {
                builder.setBolt(BOLT_OPERTION_TYPE.MJPEGSTREAMER.toString(),
                        new BatchJPEGInputBolt(new SlidingWindowBatcher(2, frameSkip)
                                .maxSize(6), new MjpegStreamingOp().useMat(false).port(8558).framerate(24),
                                BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE)
                                .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case RTMPSTREAMER: {
                builder.setBolt(BOLT_OPERTION_TYPE.RTMPSTREAMER.toString(),
                        new SingleJPEGInputBolt(new SingleRTMPWriterOp().appName(streamId)
                        .frameRate(23.98).bitRate(886000), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1)
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
        return this.streamId;
    }

}
