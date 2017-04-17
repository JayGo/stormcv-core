package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Descriptor;
import edu.fudan.stormcv.model.Feature;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FeatureSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.operation.OpenCVOp;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.*;
import org.opencv.core.Point;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Operation to calculate the color histogram of a {@link Frame} and returns a
 * {@link Feature} with a histogram per color channel.
 *
 * @author Corne Versloot
 */

public class ColorHistogramOp extends OpenCVOp<CVParticle> implements
        ISingleInputOperation<CVParticle> {

    private static final Logger logger = LoggerFactory.getLogger(ColorHistogramOp.class);
    private String stream;
    private int[] chansj = new int[]{0, 1, 2};
    private int[] histsizej = new int[]{255, 255, 255};
    private float[] rangesj = new float[]{0, 256, 0, 256, 0, 256};
    private Boolean outputFrame = true;
    private CVParticleSerializer serializer = new FeatureSerializer();
    private boolean useMat = false;

    public ColorHistogramOp(String stream) {
        this.stream = stream;
    }

    public ColorHistogramOp useMat(boolean useMat) {
        this.useMat = useMat;
        return this;
    }

    public ColorHistogramOp outputFrame(Boolean outFrame) {
        this.outputFrame = outFrame;
        if (outputFrame) {
            this.serializer = new FrameSerializer();
        } else {
            this.serializer = new FeatureSerializer();
        }
        return this;
    }

    /**
     * Configure the HistorgramOperation. The default is set for use of RGB
     * images
     *
     * @param chans    list with channal id's default = {0, 1, 2}
     * @param histsize for each channel the number of bins to use, default = {255,
     *                 255 ,255}
     * @param ranges   for each channel the min. and max. values present, default =
     *                 {0, 256, 0, 256, 0, 256 }
     * @see <a
     * href="http://docs.opencv.org/2.4.8/modules/imgproc/doc/histograms.html">OpenCV
     * Documentation</a>
     */
    public ColorHistogramOp configure(int[] chans, int[] histsize,
                                      float[] ranges) {
        chansj = chans;
        histsizej = histsize;
        rangesj = ranges;
        return this;
    }

    @Override
    protected void prepareOpenCVOp(Map stormConf, TopologyContext context)
            throws Exception {
    }

    @Override
    public void deactivate() {

    }

    @SuppressWarnings("unchecked")
    @Override
    public CVParticleSerializer<CVParticle> getSerializer() {
        return this.serializer;
    }


    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);

        List<CVParticle> result = new ArrayList<>();
        List<Descriptor> hist_descriptors = null;
        String streamId = particle.getStreamId();
        long sequenceNr = particle.getSequenceNr();

        if (useMat) {
            Mat matImage = (Mat) codecHandler.getDecodedData();

            if (matImage == null) return result;
            Mat hist = new Mat();
            MatOfInt chans;
            MatOfInt histsize;
            MatOfFloat ranges;
            List<Mat> images = new ArrayList<>();

            Rectangle box = new Rectangle(0, 0, (int) matImage.size().width,
                    (int) matImage.size().height);
            images.add(matImage);

            hist_descriptors = new ArrayList<>();

            for (int i = 0; i < chansj.length; i++) {
                chans = new MatOfInt(chansj[i]);
                histsize = new MatOfInt(histsizej[i]);
                ranges = new MatOfFloat(rangesj[i * 2], rangesj[i * 2 + 1]);
                Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

                float[] tmp = new float[1];
                int rows = (int) hist.size().height;
                float[] values = new float[rows];
                int c = 0;
                float uh = matImage.height() / chansj.length;
                float uw = matImage.width() / rows;
                float maxHight = 0;
                for (int r = 0; r < rows; r++) // loop over rows/columns
                {
                    hist.get(r, c, tmp);
                    values[r] = tmp[0];
                    if (tmp[0] > maxHight)
                        maxHight = tmp[0];
                }

                for (int r = 0; r < rows; ++r) {
                    Scalar scalar = new Scalar((i == 2 ? r : 0), (i == 1 ? r : 0), (i == 0 ? r : 0));
                    int x0 = (int) (r * uw);
                    int y0 = (int) ((i + 1) * uh - values[r] / maxHight
                            * uh);
                    int x1 = x0 + (int) uw;
                    int y1 = y0 + (int) (values[r] / maxHight * uh);

                    Core.rectangle(matImage, new Point(x0, y0), new Point(x1, y1), scalar);
                }
                hist_descriptors.add(new Descriptor(streamId, sequenceNr, box, 0, values));
            }
            frame.swapImageBytes(codecHandler.getEncodedData(matImage, frame.getImageType()));

            hist.release();
            matImage.release();

        } else {
            BufferedImage bufferedImage = (BufferedImage) codecHandler.getDecodedData();

            MatOfByte mob = new MatOfByte(frame.getImageBytes());
            Mat matImage = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);

            Mat hist = new Mat();
            MatOfInt chans;
            MatOfInt histsize;
            MatOfFloat ranges;
            List<Mat> images = new ArrayList<>();

            Rectangle box = new Rectangle(0, 0, (int) matImage.size().width,
                    (int) matImage.size().height);
            images.add(matImage);

            hist_descriptors = new ArrayList<>();

            for (int i = 0; i < chansj.length; i++) {
                chans = new MatOfInt(chansj[i]);
                histsize = new MatOfInt(histsizej[i]);
                ranges = new MatOfFloat(rangesj[i * 2], rangesj[i * 2 + 1]);
                Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

                float[] tmp = new float[1];
                int rows = (int) hist.size().height;
                float[] values = new float[rows];
                int c = 0;
                float uh = matImage.height() / chansj.length;
                float uw = matImage.width() / rows;
                float maxHight = 0;
                for (int r = 0; r < rows; r++) // loop over rows/columns
                {
                    hist.get(r, c, tmp);
                    values[r] = tmp[0];
                    if (tmp[0] > maxHight)
                        maxHight = tmp[0];
                }


                Graphics2D graphics = bufferedImage.createGraphics();
                for (int r = 0; r < rows; ++r) {
                    Color color = new Color((i == 0 ? r : 0), (i == 1 ? r : 0),
                            (i == 2 ? r : 0));
                    graphics.setColor(color);
                    graphics.drawRect((int) (r * uw),
                            (int) ((i + 1) * uh - values[r] / maxHight
                                    * uh), (int) uw,
                            (int) (values[r] / maxHight * uh));
                }
                hist_descriptors.add(new Descriptor(streamId, sequenceNr, box, 0, values));
            }
            frame.swapImageBytes(codecHandler.getEncodedData(bufferedImage, frame.getImageType()));
        }

        Feature feature = new Feature(particle.getStreamId(),
                particle.getSequenceNr(), stream, 0, hist_descriptors, null);

        // add features to result
        if (hist_descriptors.size() > 0) {
            if (outputFrame) {
                frame.getFeatures().add(feature);
                result.add(frame);
            } else {
                result.add(feature);
            }
        }
        return result;
    }

    private List<Descriptor> getColorhistogramDescriptor(Mat matImage, String streamId, long sequenceNr) {
        Mat hist = new Mat();
        MatOfInt chans;
        MatOfInt histsize;
        MatOfFloat ranges;
        List<Mat> images = new ArrayList<>();

        Rectangle box = new Rectangle(0, 0, (int) matImage.size().width,
                (int) matImage.size().height);
        images.add(matImage);

        List<Descriptor> hist_descriptors = new ArrayList<>();

        for (int i = 0; i < chansj.length; i++) {
            chans = new MatOfInt(chansj[i]);
            histsize = new MatOfInt(histsizej[i]);
            ranges = new MatOfFloat(rangesj[i * 2], rangesj[i * 2 + 1]);
            Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

            float[] tmp = new float[1];
            int rows = (int) hist.size().height;
            float[] values = new float[rows];
            int c = 0;

            float maxHight = 0;
            for (int r = 0; r < rows; r++) // loop over rows/columns
            {
                hist.get(r, c, tmp);
                values[r] = tmp[0];
                if (tmp[0] > maxHight)
                    maxHight = tmp[0];
            }


//            Graphics2D graphics = bufferedImage.createGraphics();
//            for (int r = 0; r < rows; ++r) {
//                Color color = new Color((i == 0 ? r : 0), (i == 1 ? r : 0),
//                        (i == 2 ? r : 0));
//                graphics.setColor(color);
//                graphics.drawRect((int) (r * uw),
//                        (int) ((i + 1) * uh - values[r] / maxHight
//                                * uh), (int) uw,
//                        (int) (values[r] / maxHight * uh));
//            }
            hist_descriptors.add(new Descriptor(streamId, sequenceNr, box, 0, values));
        }

        return hist_descriptors;
    }
}
