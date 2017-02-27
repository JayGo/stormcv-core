package edu.fudan.stormcv.spout;

import edu.fudan.stormcv.model.MatImage;
import edu.fudan.stormcv.model.serializer.MatImageSerializer;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.opencv.core.Mat;
import org.opencv.highgui.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class MatSpout implements IRichSpout {

    private static final long serialVersionUID = 3722073730881087403L;
    protected SpoutOutputCollector collector;
    private VideoCapture capture;
    private String videoAddr;
    private String streamId;
    private MatImageSerializer mMatImageSerializer = new MatImageSerializer();
    ;
    private Logger logger = LoggerFactory.getLogger(MatSpout.class);

    private long seq;

    public MatSpout(String videoAddr, String streamId) {
        this.videoAddr = videoAddr;
        this.streamId = streamId;
        seq = 0;
    }

    @Override
    public void ack(Object arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void fail(Object paramObject) {
        // TODO Auto-generated method stub
        logger.info(new Date().toString() + " Failed. Object " + paramObject);
    }

    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub
        if (!capture.isOpened()) {
            capture.open(videoAddr);
        }
        Mat image = new Mat();
        capture.read(image);
        if (!image.empty()) {
            MatImage matImage = new MatImage(image, streamId, image.cols(), image.rows(), System.currentTimeMillis(), seq, image.type());
            seq++;
            Values values;
            try {
                values = mMatImageSerializer.toTuple(matImage);
                String id = matImage.getStreamId() + "_"
                        + matImage.getSequence();
                collector.emit(values, id);

                matImage.release();
                matImage = null;
                System.gc();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            LibLoader.loadOpenCVLib();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        capture = new VideoCapture(videoAddr);
        capture.open(videoAddr);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(mMatImageSerializer.getFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
