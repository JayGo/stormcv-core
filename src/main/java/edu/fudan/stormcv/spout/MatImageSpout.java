//package nl.tno.stormcv.spout;
//
//import java.io.IOException;
//import java.util.Map;
//import IFetcher;
//import nl.tno.stormcv.fetcher.OpenCVStreamFrameFetcher2;
//import CVParticle;
//import MatImage;
//import MatImageSerializer;
//import LibLoader;
//
//import org.apache.storm.spout.SpoutOutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.IRichSpout;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.tuple.Values;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
///**
// * Basic spout implementation that includes fault tolerance (if activated). It should be noted that running the spout in fault
// * tolerant mode will use more memory because emitted tuples are cashed for a limited amount of time (which is configurable).
// *
// * THe actual reading of data is done by {@link IFetcher} implementations creating {@link CVParticle} objects which are serialized
// * and emitted by this spout.
// *
// * @author Corne Versloot
// */
//public class MatImageSpout implements IRichSpout {
//
//	private static final long serialVersionUID = -191782564081654220L;
//	private Logger logger = LoggerFactory.getLogger(MatImageSpout.class);
//	protected SpoutOutputCollector collector;
//    private OpenCVStreamFrameFetcher2 fetcher;
//
//	public MatImageSpout(OpenCVStreamFrameFetcher2 fetcher) {
//		this.fetcher = fetcher;
//	}
//
//
//	@Override
//	public void open(Map conf, TopologyContext context,
//			SpoutOutputCollector collector) {
//		this.collector = collector;
//		// pass configuration to subclasses
//		try {
//			fetcher.prepare(conf, context);
//			LibLoader.loadOpenCVLib();
//		} catch (IOException e) {
//			logger.error("Unable to load opencv dynamic lib due to e", e);
//		} catch (Exception e) {
//			logger.warn("Unable to configure spout due to ", e);
//		}
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(this.getSerializer().getFields());
//		//declarer.declare(serializer.getFields());
//	}
//
//	@Override
//	public void nextTuple() {
//		MatImage matImage = fetcher.fetchData2();
//
//		if (matImage != null) {
//			try {
//				Values values = this.getSerializer().toTuple(matImage);
//				String id = matImage.getStreamId() + "_"
//						+ matImage.getSequence();
//				collector.emit(values, id);
//			} catch (IOException e) {
//				logger.warn("Unable to fetch next frame from queue due to: "
//						+ e.getMessage());
//			}
//		}
//	}
//
//	@Override
//	public void close() {
//		fetcher.deactivate();
//	}
//
//	@Override
//	public void activate() {
//		fetcher.activate();
//	}
//
//	@Override
//	public void deactivate() {
//		fetcher.deactivate();
//	}
//
//	@Override
//	public void ack(Object msgId) {
//		return;
//	}
//
//	@Override
//	public void fail(Object msgId) {
//		logger.debug("Fail of: "+msgId);
//	}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		return null;
//	}
//
//	public MatImageSerializer getSerializer() {
//		return new MatImageSerializer();
//	}
//
//}
