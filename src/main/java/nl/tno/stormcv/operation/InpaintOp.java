package nl.tno.stormcv.operation;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jkyan on 3/5/16.
 */
public class InpaintOp implements ISingleInputOperation<Frame> {

    @SuppressWarnings("unused")
    private Logger logger = LoggerFactory.getLogger(GrayscaleOp.class);
    private FrameSerializer serializer = new FrameSerializer();
    private Inpaint2 inpainter;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        inpainter = new Inpaint2();
    }

    @Override
    public List<Frame> execute(CVParticle particle) throws IOException {
        List<Frame> result = new ArrayList<Frame>();
        Frame sf = (Frame) particle;
        BufferedImage image = sf.getImage();

        long startMili = System.currentTimeMillis();
        System.out.println("start:ʼ " + startMili);
        BufferedImage buffer = inpainter.init(image, image, true);
        long endMili = System.currentTimeMillis();
        System.out.println("end:" + endMili);
        System.out.println("total:" + (endMili - startMili) + "s");

        sf.setImage(buffer);
        result.add(sf);
        return result;
    }

    @Override
    public void deactivate() { }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }

}
