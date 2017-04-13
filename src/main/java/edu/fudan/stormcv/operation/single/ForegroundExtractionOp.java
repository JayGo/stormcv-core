package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.ForegroundExtractionJNI;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A simple operation that extract foreground of the image within received {@link Frame} objects to gray
 * using jliu's Cplusplus function. You need load the generated dynamic link file.
 *
 * @author jkyan, jliu
 */
public class ForegroundExtractionOp implements ISingleInputOperation<Frame> {
    private Logger logger = LoggerFactory.getLogger(ForegroundExtractionOp.class);
    private FrameSerializer serializer = new FrameSerializer();

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws RuntimeException, IOException {
        String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
        String foreground_lib = "foreground_extraction.so";
        try {
            System.load(userDir + "/" + foreground_lib);
        } catch (UnsatisfiedLinkError e) {
            logger.error("Cannot load foreground so library!");
        }
        LibLoader.loadOpenCVLib();
    }

    @Override
    public void deactivate() {
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }


    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<Frame> result = new ArrayList<>();
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        BufferedImage image = (BufferedImage) codecHandler.getDecodedData();
        int height = image.getHeight();
        int width = image.getWidth();

        byte[] initBytes = frame.getImageBytes();

        if (initBytes == null) return result;

        byte[] processedBytes = null;
        processedBytes = ForegroundExtractionJNI.foregroundExtract(initBytes,
                height, width);
        if (processedBytes == null) return result;

        frame.swapImageBytes(codecHandler.getEncodedData(processedBytes, frame.getImageType()));

        result.add(frame);
        return result;
    }
}
