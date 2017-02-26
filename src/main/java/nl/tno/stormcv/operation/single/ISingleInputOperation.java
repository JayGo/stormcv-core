package nl.tno.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IOperation;

import java.util.List;

/**
 * Interface for {@link IOperation}'s that work on a single {@link CVParticle} as input.
 *
 * @author Corne Versloot
 */
public interface ISingleInputOperation<Output extends CVParticle> extends IOperation<Output> {
    //List<Output> execute(CVParticle particle) throws Exception;
    String getContext();

    List<Output> execute(CVParticle particle, OperationHandler operationHandler) throws Exception;
}
