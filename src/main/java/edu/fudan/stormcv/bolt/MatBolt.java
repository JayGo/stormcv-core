package edu.fudan.stormcv.bolt;

import edu.fudan.stormcv.model.MatImage;
import edu.fudan.stormcv.model.serializer.MatImageSerializer;
import edu.fudan.stormcv.operation.single.IMatOperation;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MatBolt extends BaseRichBolt {

    protected Logger logger = LoggerFactory.getLogger(MatBolt.class);
    protected OutputCollector collector;
    private MatImageSerializer serializer = new MatImageSerializer();
    private IMatOperation<MatImage> mIMatOperation;

    public MatBolt(IMatOperation<MatImage> mIMatOperation) {
        this.mIMatOperation = mIMatOperation;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        try {
            LibLoader.loadOpenCVLib();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            MatImage image = serializer.fromTuple(input);

            logger.debug("image " + image.getSequence() + " : " + image.getWidth() + "x" + image.getHeight() + " type " + image.getType()
                    + " byte size " + image.getMatbytes().length + " with " + image.getMat().cols() + "x" + image.getMat().rows());

            if (image != null) {
                List<MatImage> matImages = mIMatOperation.execute(image);
                for (MatImage matImage : matImages) {
                    if (matImage != null) {
                        collector.emit(input, serializer.toTuple(matImage));
                        collector.ack(input);
                    }
                }

            }
        } catch (Exception e) {

            logger.warn("Unable to process input due to ", e);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(serializer.getFields());
    }
}
