package edu.fudan.stormcv.topology;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.lwang.codec.SourceInfo;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchH264InputBolt;
import edu.fudan.stormcv.bolt.SingleH264InputBolt;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.ColorHistogramOp;
import edu.fudan.stormcv.operation.single.GrayImageOp;
import edu.fudan.stormcv.operation.single.H264RtmpStreamOp;
import edu.fudan.stormcv.operation.single.HaarCascadeOp;
import edu.fudan.stormcv.spout.TCPCaptureSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.commons.cli.*;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayScaleTopologyH264WithOpts extends BaseTopology {

    private static final Logger logger = LoggerFactory.getLogger(GrayScaleTopologyH264WithOpts.class);
    private String streamId = "GrayScaleTopologyH264";
    private String videoAddr;
    private String rtmpAddr = GlobalConstants.DefaultRTMPServer;
    private SourceInfo sourceInfo;
    private int frameSkip = 0;
    private BOLT_OPERTION_TYPE type;


    private boolean sendRtmp = true;
    private boolean view720p = true;
    private CommandLineParser parser;
    private Options options;
    private int workerNum = 2;

    public static void main(String[] args) {
        LibLoader.loadOpenCVLib();
        LibLoader.loadRtmpStreamerLib();

        GrayScaleTopologyH264WithOpts topology = new GrayScaleTopologyH264WithOpts(BOLT_OPERTION_TYPE.COLORHISTOGRAM);
        topology.parseCommandArgs(args);

        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public GrayScaleTopologyH264WithOpts(BOLT_OPERTION_TYPE type) {
        conf.setNumWorkers(workerNum);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.X264_IMAGE);
        this.type = type;
        isTopologyRunningAtLocal = true;

        if (view720p) {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
                videoAddr = GlobalConstants.Pseudo720pFaceRtspAddress;
            } else {
                videoAddr = GlobalConstants.Pseudo720pRtspAddress;
            }
        } else {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
                videoAddr = GlobalConstants.PseudoFaceRtspAddress;
            } else {
                videoAddr = GlobalConstants.PseudoRtspAddress;
            }
        }

        this.parser = new DefaultParser();
        this.options = new Options();
        initOptions();
    }

    public GrayScaleTopologyH264WithOpts(String streamId, String rtmpAddr, String videoAddr) {
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

    private void initOptions() {
        options.addOption("cl", "cluster", false, "cluster mode");
        Option workerNumOption = Option.builder("w")
                .hasArgs()
                .desc("workerNum")
                .build();
        options.addOption(workerNumOption);
        options.addOption("rtmp", "rtmp", false, "send stream to rtmp");
        options.addOption("720p", "720p", false, "process 720p video");
    }

    private void parseCommandArgs(String[] args) {
        CommandLine commandLine = null;
        try {
            commandLine = this.parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (commandLine.hasOption("cl")) {
            isTopologyRunningAtLocal = false;
        }

        if (commandLine.hasOption("rtmp")) {
            sendRtmp = true;
        }

        if (commandLine.hasOption("720p")) {
            view720p = true;
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
                videoAddr = GlobalConstants.Pseudo720pFaceRtspAddress;
            } else {
                videoAddr = GlobalConstants.Pseudo720pRtspAddress;
            }
        }
        if (commandLine.hasOption("w")) {
            this.workerNum =  Integer.valueOf(commandLine.getOptionValue("w"));
        }
    }

    @Override
    public String getStreamId() {
        return streamId;
    }


}
