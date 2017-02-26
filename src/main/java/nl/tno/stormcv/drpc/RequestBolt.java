package nl.tno.stormcv.drpc;

import nl.tno.stormcv.model.CVParticle;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RequestBolt extends BaseRichBolt {

    private static final long serialVersionUID = 6796738184673678040L;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private IRequestOp<? extends CVParticle> operation;
    private OutputCollector collector;

    public RequestBolt(IRequestOp<? extends CVParticle> operation) {
        this.operation = operation;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            operation.prepare(conf, context);
        } catch (Exception e) {
            logger.error("Unable to prepare CVParticleBolt due to ", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long requestId = tuple.getLong(0);
            List<? extends CVParticle> result = operation.execute(requestId, tuple.getString(1));
            for (CVParticle particle : result) {
                particle.setRequestId(requestId);
                collector.emit(tuple, operation.getSerializer().toTuple(particle));
            }
            collector.ack(tuple);
        } catch (IOException ioe) {
            logger.error("Unable to serialize result to tuple", ioe);
            collector.fail(tuple);
        } catch (Exception e) {
            logger.error("Unable to proces tuple", e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(operation.getSerializer().getFields());
    }

}