package nl.tno.stormcv.bolt;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.ISingleInputOperation;
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
public class SingleInputBolt extends CVParticleBolt {

    private static final long serialVersionUID = 8954087163234223475L;

    private ISingleInputOperation<? extends CVParticle> operation;

    /**
     * Constructs a SingleInputOperation
     * @param operation the operation to be performed
     */
    public SingleInputBolt(ISingleInputOperation<? extends CVParticle> operation){
        this.operation = operation;
    }



    @SuppressWarnings("rawtypes")
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
    List<? extends CVParticle> execute(CVParticle input) throws Exception{

        List<? extends CVParticle> result = operation.execute(input);
        // copy metadata from input to output if configured to do so
        for(CVParticle s : result){
            for(String key : input.getMetadata().keySet()){
                if(!s.getMetadata().containsKey(key)){
                    s.getMetadata().put(key, input.getMetadata().get(key));
                }
            }
        }
        return result;
    }
}
