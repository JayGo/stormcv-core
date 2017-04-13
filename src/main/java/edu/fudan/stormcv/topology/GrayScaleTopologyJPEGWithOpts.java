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
import org.apache.commons.cli.*;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:24 AM
 * Description:
 */
public class GrayScaleTopologyJPEGWithOpts extends BaseTopology {

    private int frameSkip = 0;
    private List<String> urls;
    private String streamId = "GrayScaleTopologyJPEG";
    private BOLT_OPERTION_TYPE type;

    private boolean view720p = false;
    private boolean sendRtmp = false;
    private CommandLineParser parser;
    private Options options;
    private int workerNum = 2;

    public static void main(String[] args) {
        GrayScaleTopologyJPEGWithOpts topology = new GrayScaleTopologyJPEGWithOpts(BOLT_OPERTION_TYPE.SCALE);
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public GrayScaleTopologyJPEGWithOpts(BOLT_OPERTION_TYPE type) {
        conf.setNumWorkers(workerNum);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        urls = new ArrayList<>();
        this.type = type;

        isTopologyRunningAtLocal = true;
        this.parser = new DefaultParser();
        this.options = new Options();
        initOptions();
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
                        new SingleJPEGInputBolt(new SingleRTMPWriterOp().appName("grayscale")
                        .frameRate(23.98).bitRate(886000), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE), 1)
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
        }
        if (commandLine.hasOption("w")) {
            this.workerNum =  Integer.valueOf(commandLine.getOptionValue("w"));
        }

        if (view720p) {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
//                urls.add(GlobalConstants.Pseudo720pFaceRtspAddress);
            } else {
                urls.add(GlobalConstants.Pseudo720pRtspAddress);
            }
        } else {
            if (this.type == BOLT_OPERTION_TYPE.FACEDETECT) {
//                urls.add(GlobalConstants.PseudoFaceRtspAddress);
            } else {
                urls.add(GlobalConstants.PseudoRtspAddress);
            }
        }
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }

}
