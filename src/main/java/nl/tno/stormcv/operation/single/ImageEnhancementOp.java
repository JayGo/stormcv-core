package nl.tno.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.OpenCVOp;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImageEnhancementOp extends OpenCVOp<CVParticle> implements ISingleInputOperation<CVParticle> {
    private CVParticleSerializer serializer = new FrameSerializer();
    private String name;


    public ImageEnhancementOp(String name) {
        this.name = name;
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public CVParticleSerializer<CVParticle> getSerializer() {
        // TODO Auto-generated method stub
        return serializer;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void prepareOpenCVOp(Map stormConf, TopologyContext context)
            throws Exception {

    }

    static public Mat doEnhancement(Mat in) {
        Mat out = new Mat(in.height(), in.width(), CvType.CV_8UC3);

        // Mat(RGB) convert to Mat(YCbCr)

        Mat yCbCr = new Mat();
        Imgproc.cvtColor(in, yCbCr, Imgproc.COLOR_BGR2YCrCb);

        List<Mat> yCbCrList = new ArrayList<Mat>();
        Core.split(yCbCr, yCbCrList);

        // Get Y channel mat
        Mat y = yCbCrList.get(0);

        byte yData[] = new byte[y.height() * y.width()];
        y.get(0, 0, yData);

        // System.out.println("after get:" + y.width());
        // System.out.println("after get:" + y.height());

        double p[] = new double[256];
        double p1[] = new double[256];

        for (int i = 0; i < y.height(); i++) {
            for (int j = 0; j < y.width(); j++) {
                // System.out.println(yData[j+i*y.width()] & 0xff);
                p[yData[j + i * y.width()] & 0x0ff]++;
            }
        }

        int imgSize = y.height() * y.width();

        for (int i = 0; i < 256; i++) {
            p[i] = p[i] / imgSize;
        }

        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < i; j++) {
                p1[i] += p[j];
            }
        }

        byte yDataNew[] = new byte[imgSize];

        for (int i = 0; i < y.height(); i++) {
            for (int j = 0; j < y.width(); j++) {
                int index = j + i * y.width();
                yDataNew[index] = (byte) (255 * p1[yData[index] & 0x0ff] + 0.5);
            }
        }


        y.put(0, 0, yDataNew);

        yCbCrList.set(0, y);

        Core.merge(yCbCrList, yCbCr);

        Imgproc.cvtColor(yCbCr, out, Imgproc.COLOR_YCrCb2BGR);

        return out;
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        Frame frame = (Frame) particle;

        List<CVParticle> results = new ArrayList<CVParticle>();

        codecHandler.fillSourceBufferQueue(frame);

        Mat input = (Mat) codecHandler.getDecodedData();

        Mat output = doEnhancement(input);

        frame.swapImageBytes(codecHandler.getEncodedData(output));

        results.add(frame);

        return results;
    }


}
