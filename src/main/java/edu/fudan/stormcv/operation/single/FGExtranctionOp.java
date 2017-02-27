package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.FGEHelper;
import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author lwang
 */
public class FGExtranctionOp implements ISingleInputOperation<Frame> {

    private FrameSerializer serializer = new FrameSerializer();
    private static FGEHelper mFGEHelper = new FGEHelper();
    private Logger logger = LoggerFactory.getLogger(FGExtranctionOp.class);
    private boolean isLoadLibrary = true;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context)
            throws Exception {
        // TODO Auto-generated method stub
        isLoadLibrary = LibLoader.loadCudartLib() && LibLoader.loadBayesFGELib();
//		logger.info("load library result : " + LibLoader.loadCudartLib() + "/"  +  LibLoader.loadBayesFGELib());
        if (isLoadLibrary) {
            mFGEHelper.init0(1920, 1080, 3);
        }
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        // TODO Auto-generated method stub
        return serializer;
    }

    @Override
    public String getContext() {
        // TODO Auto-generated method stub
        return this.getClass().getSimpleName();
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<Frame> result = new ArrayList<Frame>();
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        Mat in = (Mat) codecHandler.getDecodedData();
//		logger.info("in image " + frame.getSequenceNr() + " : " + in.width() + "x" + in.height() + " dims " + in.dims());
        Mat out = null;
        if (isLoadLibrary) {
            out = new Mat(new FGEHelper().getFGImage(in.nativeObj));
        }
        if (out == null) {
            return result;
        }
        byte[] encodedBytes = codecHandler.getEncodedData(out);
        frame.swapImageBytes(encodedBytes);
        result.add(frame);
        return result;
    }


}
