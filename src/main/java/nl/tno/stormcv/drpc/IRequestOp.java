package nl.tno.stormcv.drpc;


import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.operation.IOperation;

import java.util.List;

public interface IRequestOp<Output extends CVParticle> extends IOperation<Output> {

    public List<Output> execute(long requestId, String args) throws Exception;
}
