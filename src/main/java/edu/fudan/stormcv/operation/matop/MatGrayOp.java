package edu.fudan.stormcv.operation.matop;

import edu.fudan.stormcv.model.MatImage;
import edu.fudan.stormcv.model.serializer.MatImageSerializer;
import edu.fudan.stormcv.operation.single.IMatOperation;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MatGrayOp implements IMatOperation<MatImage> {

    private static final long serialVersionUID = -4101402095941832384L;

    private MatImageSerializer mMatImageSerializer = new MatImageSerializer();


    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context)
            throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public MatImageSerializer getSerializer() {
        // TODO Auto-generated method stub
        return mMatImageSerializer;
    }

    @Override
    public List<MatImage> execute(MatImage image) throws Exception {
        // TODO Auto-generated method stub
        List<MatImage> images = new ArrayList<MatImage>();

        Mat img = image.getMat();
        Mat out = new Mat(img.height(), img.width(), CvType.CV_8UC1);
        Imgproc.cvtColor(img, out, Imgproc.COLOR_BGR2GRAY);

        image.setMat(out);
        images.add(image);
        return images;
    }

}
