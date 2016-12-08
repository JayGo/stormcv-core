package nl.tno.stormcv.bolt;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.MatImage;
import nl.tno.stormcv.model.serializer.MatImageSerializer;
import nl.tno.stormcv.operation.IMatOperation;
import nl.tno.stormcv.util.NativeUtils;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5655680880310767907L;
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
			NativeUtils.load();
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		try {
			MatImage image = serializer.fromTuple(input);

			System.out.println("image " + image.getSequence() + " : " + image.getWidth() + "x" + image.getHeight() + " type " + image.getType()
					+ " byte size " + image.getMatbytes().length + " with " + image.getMat().cols() + "x" + image.getMat().rows()) ;
			
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
