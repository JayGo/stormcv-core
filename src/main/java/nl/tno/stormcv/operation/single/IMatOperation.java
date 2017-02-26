package nl.tno.stormcv.operation.single;

import nl.tno.stormcv.model.serializer.MatImageSerializer;
import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface IMatOperation<T> extends Serializable {
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
     * Provides the {@link MatImageSerializer} implementation to be used to serialize the output of the {@link IMatOperation}
     *
     * @return
     */
    public MatImageSerializer getSerializer();

    public List<T> execute(T image) throws Exception;
}
