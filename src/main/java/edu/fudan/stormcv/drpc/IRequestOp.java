package edu.fudan.stormcv.drpc;


import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.operation.IOperation;

import java.util.List;

public interface IRequestOp<Output extends CVParticle> extends IOperation<Output> {

    public List<Output> execute(long requestId, String args) throws Exception;
}
