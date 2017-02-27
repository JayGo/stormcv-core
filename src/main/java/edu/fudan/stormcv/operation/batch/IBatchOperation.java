package edu.fudan.stormcv.operation.batch;

import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.operation.IOperation;
import edu.fudan.stormcv.operation.single.ISingleInputOperation;

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
