package edu.fudan.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchH264InputBolt;
import edu.fudan.stormcv.bolt.BatchJPEGInputBolt;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.spout.TCPCaptureSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayScaleTopologyH264 extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(GrayScaleTopologyH264.class);
    private String streamId = "GrayScaleTopologyH264";
    private String videoAddr;
    private String rtmpAddr = GlobalConstants.DefaultRTMPServer;
    private SourceInfo sourceInfo;
    private int frameSkip = 0;
    private BOLT_OPERTION_TYPE type;

    private boolean sendRtmp = true;
    private boolean view720p = true;

    public static void main(String[] args) {
        LibLoader.loadOpenCVLib();
        LibLoader.loadRtmpStreamerLib();

        GrayScaleTopologyH264 topology = new GrayScaleTopologyH264(BOLT_OPERTION_TYPE.COLORHISTOGRAM);
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public GrayScaleTopologyH264(BOLT_OPERTION_TYPE type) {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.X264_IMAGE);
        this.type = type;
        isTopologyRunningAtLocal = true;

        if (view720p) {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
//                videoAddr = GlobalConstants.Pseudo720pFaceRtspAddress;
            } else {
                videoAddr = GlobalConstants.Pseudo720pRtspAddress;
            }
        } else {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
//                videoAddr = GlobalConstants.PseudoFaceRtspAddress;
            } else {
                videoAddr = GlobalConstants.PseudoRtspAddress;
            }
        }
    }

    public GrayScaleTopologyH264(String streamId, String rtmpAddr, String videoAddr) {
        this.streamId = streamId;
        this.rtmpAddr = rtmpAddr;
        this.videoAddr = videoAddr;
        conf.setNumWorkers(2);
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
            createOperationBolt(BOLT_OPERTION_TYPE.MJPEGSTREAMER, type.toString());
        } else {
            createOperationBolt(BOLT_OPERTION_TYPE.RTMPSTREAMER, type.toString());
        }
    }

    public void createOperationBolt(BOLT_OPERTION_TYPE type, String sourceComp) {
        switch (type) {
            case GRAY: {
                builder.setBolt(BOLT_OPERTION_TYPE.GRAY.toString(),
                        new SingleH264InputBolt(new GrayImageOp()).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            /*scale not supported*/
//            case SCALE: {
//                builder.setBolt(BOLT_OPERTION_TYPE.SCALE.toString(),
//                        new SingleH264InputBolt(new ScaleImageOp(0.5f).useMat(true)).setSourceInfo(sourceInfo), 1)
//                        .localOrShuffleGrouping(sourceComp);
//                break;
//            }
            case COLORHISTOGRAM: {
                builder.setBolt(BOLT_OPERTION_TYPE.COLORHISTOGRAM.toString(),
                        new SingleH264InputBolt(new ColorHistogramOp(streamId)
                                .useMat(true).outputFrame(true)).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case FACEDETECT: {
                builder.setBolt(BOLT_OPERTION_TYPE.FACEDETECT.toString(),
                        new SingleH264InputBolt(new HaarCascadeOp(streamId, GlobalConstants.HaarCacascadeXMLFileName)
                                .useMat(true).outputFrame(true)).setSourceInfo(sourceInfo), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case MJPEGSTREAMER: {
                builder.setBolt(BOLT_OPERTION_TYPE.MJPEGSTREAMER.toString(),
                        new BatchH264InputBolt(new SlidingWindowBatcher(2, frameSkip)
                                .maxSize(6), new MjpegStreamingOp()
                                .useMat(true).port(8558).framerate(24)).setSourceInfo(sourceInfo)
                                .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                        .localOrShuffleGrouping(sourceComp);
                break;
            }
            case RTMPSTREAMER: {
                builder.setBolt(BOLT_OPERTION_TYPE.RTMPSTREAMER.toString(),
                        new SingleH264InputBolt(new H264RtmpStreamOp().RTMPServer(rtmpAddr).appName("grayscale")
                                .frameRate(23.98)).setSourceInfo(sourceInfo), 1)
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
