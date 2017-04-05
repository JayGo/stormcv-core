package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.operation.OpenCVOp;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CannyEdgeOp extends OpenCVOp<CVParticle> implements
        ISingleInputOperation<CVParticle> {

    private Logger logger = LoggerFactory.getLogger(GrayImageOp.class);
    private CVParticleSerializer serializer = new FrameSerializer();
    private String name;

    public CannyEdgeOp(String name) {
        this.name = name;
    }

    @Override
    protected void prepareOpenCVOp(Map stormConf, TopologyContext context)
            throws Exception {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public CVParticleSerializer<CVParticle> getSerializer() {
        return serializer;
    }


    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws IOException {
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        Mat input = (Mat) codecHandler.getDecodedData();

        Mat inputGray = new Mat();
        Imgproc.cvtColor(input, inputGray, Imgproc.COLOR_BGR2GRAY);

        Mat output = new Mat();
        List<CVParticle> results = new ArrayList<>();

        Imgproc.Canny(inputGray, output, 255, 500);

        Mat output3Bytes = new Mat();
        Imgproc.cvtColor(output, output3Bytes, Imgproc.COLOR_GRAY2BGR);

        byte[] encodedData = codecHandler.getEncodedData(output3Bytes, frame.getImageType());
        frame.swapImageBytes(encodedData);
        results.add(frame);

        return results;
    }

}
