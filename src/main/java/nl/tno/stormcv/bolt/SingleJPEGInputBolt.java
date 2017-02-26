package nl.tno.stormcv.bolt;

import nl.tno.stormcv.constant.BOLT_HANDLE_TYPE;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.single.ISingleInputOperation;
import nl.tno.stormcv.util.OpertionHandlerFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 3:05 AM
 * Description:
 */
public class SingleJPEGInputBolt extends CVParticleBolt {

    private ISingleInputOperation<? extends CVParticle> operation;
    private BOLT_HANDLE_TYPE boltHandleType;
    private long startTime;
    private long endTime;
    private int count;

    /**
     * Constructs a SingleInputOperation
     *
     * @param operation the operation to be performed
     */
    public SingleJPEGInputBolt(ISingleInputOperation<? extends CVParticle> operation, BOLT_HANDLE_TYPE type) {
        this.operation = operation;
        this.boltHandleType = type;
        this.startTime = 0;
        this.endTime = 0;
        this.count = 0;
    }

    @Override
    void prepare(Map stormConf, TopologyContext context) {
        try {
            operation.prepare(stormConf, context);
        } catch (Exception e) {
            logger.error("Unale to prepare Operation ", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(operation.getSerializer().getFields());
    }

    @Override
    List<? extends CVParticle> execute(CVParticle input) throws Exception {
        if (this.count == 0) {
            this.startTime = System.currentTimeMillis();
        }
        List<? extends CVParticle> result = operation.execute(input, OpertionHandlerFactory.create(boltHandleType));

        this.count++;
        if (this.count == 500) {
            endTime = System.currentTimeMillis();
            logger.info("{} Rate: {}", operation.getContext(), (500 / ((endTime - startTime) / 1000.0f)));
            this.count = 0;
        }
//        for (CVParticle s : result) {
//            for (String key : input.getMetadata().keySet()) {
//                if (!s.getMetadata().containsKey(key)) {
//                    s.getMetadata().put(key, input.getMetadata().get(key));
//                }
//            }
//        }
        return result;
    }
}
