package nl.tno.stormcv.operation;

import java.util.List;

<<<<<<< HEAD
import org.opencv.core.Mat;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.lwang.codec.OperationHandler;
=======
>>>>>>> ec7e72b957babfe05e75ab37b9dd8f5295550875
import nl.tno.stormcv.model.CVParticle;

/**
 * Interface for {@link IOperation}'s that work on a single {@link CVParticle} as input.
 * 
 * @author Corne Versloot
 *
 */
public interface ISingleInputOperation <Output extends CVParticle> extends IOperation<Output>{

	public List<Output> execute(CVParticle particle) throws Exception;

	public String getContext();
	public List<Output> execute(CVParticle particle, OperationHandler operationHandler) throws Exception;


	
}
