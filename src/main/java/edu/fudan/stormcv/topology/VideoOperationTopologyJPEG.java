package edu.fudan.stormcv.topology;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.batcher.SequenceNrBatcher;
import edu.fudan.stormcv.batcher.SlidingWindowBatcher;
import edu.fudan.stormcv.bolt.BatchJPEGInputBolt;
import edu.fudan.stormcv.bolt.SingleJPEGInputBolt;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.fetcher.OpenCVStreamFrameFetcher;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.operation.batch.FramesToVideoOp;
import edu.fudan.stormcv.operation.batch.MjpegStreamingOp;
import edu.fudan.stormcv.operation.single.*;
import edu.fudan.stormcv.spout.CVParticleSpout;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.tuple.Fields;
import org.opencv.highgui.VideoCapture;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:24 AM
 * Description:
 */
public class VideoOperationTopologyJPEG extends BaseTopology {

    private int frameSkip = 0;
    private String inputLocation = "file:///home/nfs/videos/bigbang480-1.mkv";
    private String outputLocation = "file:///home/nfs/videos/output/";
    private String streamId = "VideoOperationTopologyJPEG";
    private BOLT_OPERTION_TYPE type;
    private long totalFrames;
    private double frameRate;

    public static void main(String[] args) {
        VideoOperationTopologyJPEG topology = new VideoOperationTopologyJPEG(BOLT_OPERTION_TYPE.FACEDETECT);
        try {
            topology.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public VideoOperationTopologyJPEG(BOLT_OPERTION_TYPE type) {
        conf.setNumWorkers(2);
        conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE);
        isTopologyRunningAtLocal = true;
        this.type = type;
    }

    public VideoOperationTopologyJPEG(String inputLocation, String outputLocation, BOLT_OPERTION_TYPE type) {
        this(type);
        this.inputLocation = inputLocation;
        this.outputLocation = outputLocation;
    }

    @Override
    public void setSpout() {
        List<String> urls = new ArrayList<>();
        urls.add(inputLocation);
        builder.setSpout("spout", new CVParticleSpout(new OpenCVStreamFrameFetcher(
                urls).frameSkip(frameSkip)), 1);
    }

    public void prepare() {
        LibLoader.loadOpenCVLib();
        VideoCapture capture = new VideoCapture(inputLocation);
        totalFrames = (long) capture.get(7);
        frameRate = capture.get(5);
        System.out.println("totalFrames:" + totalFrames + ", frameRate:" + frameRate);
    }

    @Override
    public void setBolts() {
        prepare();
        createOperationBolt(type, "spout");
        builder.setBolt(BOLT_OPERTION_TYPE.MJPEGSTREAMER.toString(),
                new BatchJPEGInputBolt(new SequenceNrBatcher(2), new FramesToVideoOp(outputLocation, totalFrames)
                        .bitrate(886000).speed(1.0f).frameRate(frameRate),
                        BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE)
                        .groupBy(new Fields(CVParticleSerializer.STREAMID)), 1)
                .localOrShuffleGrouping(type.toString());

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
                                .useMat(true).outputFrame(true).minSize(0, 0),
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

    @Override
    public String getStreamId() {
        return this.streamId;
    }

}
