package nl.tno.stormcv.operation.batch;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IOperation;
import nl.tno.stormcv.operation.single.ISingleInputOperation;

import java.util.List;

/**
 * Interface for batch operations which take a list with {@link CVParticle} objects as input instead
 * of a single object as {@link ISingleInputOperation} requires.
 *
 * @param <Output>
 * @author Corne Versloot
 */
public interface IBatchOperation<Output extends CVParticle> extends IOperation<Output> {

    public List<Output> execute(List<CVParticle> input) throws Exception;

}
