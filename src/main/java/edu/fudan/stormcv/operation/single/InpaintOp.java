package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jkyan on 3/5/16.
 */
public class InpaintOp implements ISingleInputOperation<Frame> {

    private static final Logger logger = LoggerFactory.getLogger(InpaintOp.class);
    private FrameSerializer serializer = new FrameSerializer();
    private Inpaint2 inpainter;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        inpainter = new Inpaint2();
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
        List<Frame> result = new ArrayList<Frame>();
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        BufferedImage image = (BufferedImage) codecHandler.getDecodedData();

        long startMili = System.currentTimeMillis();
        logger.info("start:ʼ " + startMili);
        BufferedImage buffer = inpainter.init(image, image, true);
        long endMili = System.currentTimeMillis();
        logger.info("end:" + endMili);
        logger.info("total:" + (endMili - startMili) + "s");

        frame.swapImageBytes(codecHandler.getEncodedData(buffer, frame.getImageType()));
        result.add(frame);
        return result;
    }
}
