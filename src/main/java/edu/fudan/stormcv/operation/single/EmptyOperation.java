package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmptyOperation implements ISingleInputOperation<Frame> {

    private static final Logger logger = LoggerFactory.getLogger(EmptyOperation.class);
    private String name;
    private int frameNr = 0;
    private FrameSerializer serializer = new FrameSerializer();
    private static final String H264Dir = "/root/H264Data/";

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        // TODO Auto-generated method stub

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


    public List<Frame> execute(CVParticle particle) throws Exception {
        // TODO Auto-generated method stub
        Frame frame = (Frame) particle;
//		logger.info("Receive frame: " + frame);
        List<Frame> results = new ArrayList<>();
        results.add(frame);
        return results;
    }

    @Override
    public String getContext() {
        // TODO Auto-generated method stub
        return this.getClass().getSimpleName();
    }

    public void printData(Frame frame) {
        byte[] datas = frame.getImageBytes();
        int length = datas.length;

        if (length <= 0)
            return;

        File dir = new File(H264Dir);
        if (!dir.exists() && !dir.isDirectory()) {
            if (!dir.mkdir()) {
                logger.error("Directory " + dir.getPath() + " create failed!");
            }
        }

        File file = new File(H264Dir + frameNr++);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error(e.getMessage());
        }

        if (fos == null) {
            logger.error("Null poniter exception!");
        }
        try {
            for (int i = 0; i < length; i++) {
                fos.write((datas[i] & 0x0FF));
                // logger.info(i+"'th byte value: "+(int) (datas[i]&0x0FF));
            }
            fos.flush();
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler operationHandler) throws Exception {
        // TODO Auto-generated method stub
        Frame frame = (Frame) particle;
//		logger.info("Receive frame: " + frame);

        // printData(frame);

        operationHandler.fillSourceBufferQueue(frame);

        Mat mat = (Mat) operationHandler.getDecodedData();

        if (mat != null) {
            // logger.info("Decode "+frameNr+" done");
            // Highgui.imwrite("/root/Pictures/"+ frameNr++ +".jpg", mat);
            // logger.info("image size: " + mat.width() + "x" + mat.height());
            byte[] encodedData = operationHandler.getEncodedData(mat);
            if (encodedData == null) {
                logger.error("encode data is null!!");
            }
            frame.swapImageBytes(encodedData);
        }

        List<Frame> results = new ArrayList<>();
        results.add(frame);
        return results;

    }

}
