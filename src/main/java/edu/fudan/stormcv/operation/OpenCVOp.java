package edu.fudan.stormcv.operation;

import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.TopologyContext;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract class containing basic functionality to load (custom) OpenCV libraries. The name of the library to be loaded
 * can be set though the libName function. If this name is not set it will attempt to find and load the right OpenCV
 * library automatically. It is advised to set the name of the library to use. Operations that use OpenCV can extend this
 * class and use its utility functions.
 * Operation.
 *
 * @author Corne Versloot
 */
public abstract class OpenCVOp<Output extends CVParticle> implements IOperation<Output> {
    private String libName;

    protected String getLibName() {
        return this.libName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        loadOpenCV(stormConf);
        this.prepareOpenCVOp(stormConf, context);
    }

    protected void loadOpenCV(Map stormConf) throws RuntimeException, IOException {
        this.libName = (String) stormConf.get(StormCVConfig.STORMCV_OPENCV_LIB);
        LibLoader.loadOpenCVLib();
//		if(libName == null) NativeUtils.load();
//		else NativeUtils.load(libName);
    }

    protected abstract void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception;
}
