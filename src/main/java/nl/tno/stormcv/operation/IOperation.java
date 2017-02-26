package nl.tno.stormcv.operation;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Basic interface for all operations within the platform.
 *
 * @param <Output>
 * @author Corne Versloot
 */
public interface IOperation<Output extends CVParticle> extends Serializable {

    /**
     * Called when the topology is activated and can be used to configure itself and load
     * resources required
     *
     * @param stormConf
     * @param context
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) throws Exception;

    /**
     * Called when the topology is halted and can be used to clean up resources used
     */
    public void deactivate();

    /**
     * Provides the {@link CVParticleSerializer} implementation to be used to serialize the output of the {@link IOperation}
     *
     * @return
     */
    public CVParticleSerializer<Output> getSerializer();

}
