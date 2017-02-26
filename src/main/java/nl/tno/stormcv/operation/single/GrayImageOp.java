package nl.tno.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.ColorConvertOp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A simple operation that converts the image within received {@link Frame} objects to gray
 * using Java's native {@link ColorConvertOp}. Hence the OpenCV library is not required to use this Operation.
 *
 * @author Corne Versloot
 */
public class GrayImageOp implements ISingleInputOperation<Frame> {

    private Logger logger = LoggerFactory.getLogger(GrayImageOp.class);
    private FrameSerializer serializer = new FrameSerializer();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        Mat in = (Mat) codecHandler.getDecodedData();
        if (in != null) {
            Mat out = new Mat(in.height(), in.width(), CvType.CV_8UC1);
            Imgproc.cvtColor(in, out, Imgproc.COLOR_BGR2GRAY);
            byte[] encodedData = codecHandler.getEncodedData(out);
            if (encodedData == null) {
                logger.error("encode data is null!!");
            }
            frame.swapImageBytes(encodedData);
        }
        List<Frame> results = new ArrayList<>();
        results.add(frame);
        return results;
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }
}
