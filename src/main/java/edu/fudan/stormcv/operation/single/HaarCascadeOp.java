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
import edu.fudan.stormcv.util.ImageUtils;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.*;
import org.opencv.core.Point;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Detects Haar Cascades in frames/images using OpenCV's CascadeClassifier.
 * The name of the objects detected as well as the model must be provided on construction.
 * It is possible to specify the minimum and maximum dimensions of detections as well.
 *
 * @author Corne Versloot
 * @see <a href="http://docs.opencv.org/2.4.8/modules/objdetect/doc/cascade_classification.html">OpenCV Documentation</a>
 */
public class HaarCascadeOp extends OpenCVOp<CVParticle> implements ISingleInputOperation<CVParticle> {
    private Logger logger = LoggerFactory.getLogger(HaarCascadeOp.class);
    private String name;
    private CascadeClassifier haarDetector;
    private String haarXML;
    private int[] minSize = new int[]{100, 100};
    private int[] maxSize = new int[]{1000, 1000};
    private int minNeighbors = 3;
    private int flags = 0;
    private float scaleFactor = 1.1f;
    private boolean outputFrame = true;
    private CVParticleSerializer serializer = new FeatureSerializer();
    private boolean useMat = false;

    /**
     * Constructs a {@link HaarCascadeOp} using the provided haarXML and will emit Features with the provided name
     *
     * @param name
     * @param haarXML
     */
    public HaarCascadeOp(String name, String haarXML) {
        this.name = name;
        this.haarXML = haarXML;
    }

    public HaarCascadeOp minSize(int w, int h) {
        this.minSize = new int[]{w, h};
        return this;
    }

    public HaarCascadeOp useMat(boolean useMat) {
        this.useMat = useMat;
        return this;
    }

    public HaarCascadeOp maxSize(int w, int h) {
        this.maxSize = new int[]{w, h};
        return this;
    }

    public HaarCascadeOp minNeighbors(int number) {
        this.minNeighbors = number;
        return this;
    }

    public HaarCascadeOp flags(int f) {
        this.flags = f;
        return this;
    }

    public HaarCascadeOp scale(float factor) {
        this.scaleFactor = factor;
        return this;
    }

    /**
     * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
     * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
     *
     * @param frame
     * @return
     */
    public HaarCascadeOp outputFrame(boolean frame) {
        this.outputFrame = frame;
        if (outputFrame) {
            this.serializer = new FrameSerializer();
        } else {
            this.serializer = new FeatureSerializer();
        }
        return this;
    }

    @Override
    protected void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception {
        try {
            if (haarXML.charAt(0) != '/') haarXML = "/" + haarXML;
            File cascadeFile = new File(LibLoader.getSelfJarDenpendencyFileTmp(haarXML, true));
            haarDetector = new CascadeClassifier(cascadeFile.getAbsolutePath());
//            Utils.sleep(100); // make sure the classifier has loaded before removing the tmp xml file
//            if (!cascadeFile.delete()) cascadeFile.deleteOnExit();
        } catch (Exception e) {
            logger.error("Unable to instantiate SimpleFaceDetectionBolt due to: " + e.getMessage(), e);
            throw e;
        }
        logger.info("prepared successfully for face detect");
    }

    @Override
    public void deactivate() {
        haarDetector = null;
    }

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
        ArrayList<CVParticle> result = new ArrayList<CVParticle>();
        Frame frame = (Frame) particle;
        codecHandler.fillSourceBufferQueue(frame);
        Mat image = null;
        BufferedImage frameImage = null;

        if (this.useMat) {
            image = (Mat) codecHandler.getDecodedData();
        } else {
            frameImage = (BufferedImage) codecHandler.getDecodedData();
            MatOfByte mob = new MatOfByte(frame.getImageBytes());
            image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
        }

        if (image != null) {
            //long startTime = System.currentTimeMillis();
            MatOfRect haarDetections = new MatOfRect();
            haarDetector.detectMultiScale(image, haarDetections, scaleFactor, minNeighbors, flags, new Size(minSize[0], minSize[1]), new Size(maxSize[0], maxSize[1]));
            ArrayList<Descriptor> descriptors = new ArrayList<>();

            int colorIndex = 0;
            Core.putText(image, particle.getStreamId(), new Point(20, 20), Core.FONT_HERSHEY_PLAIN, 1.0, ImageUtils.scalars[0]);
            for (Rect rect : haarDetections.toArray()) {
                Rectangle box = new Rectangle(rect.x, rect.y, rect.width, rect.height);
                descriptors.add(new Descriptor(particle.getStreamId(), particle.getSequenceNr(), box, 0, new float[0]));
                Core.rectangle(image, rect.tl(), rect.br(), ImageUtils.scalars[colorIndex % ImageUtils.scalars.length]);
                colorIndex++;
            }
            //logger.info("finish the facedetect of frame{}, time = {}ms", frame.getSequenceNr(), (System.currentTimeMillis() - startTime));
            if (this.useMat) {
                frame.swapImageBytes(codecHandler.getEncodedData(image));
            } else {
                frame.swapImageBytes(codecHandler.getEncodedData(frameImage));
            }
            Feature feature = new Feature(particle.getStreamId(), particle.getSequenceNr(), name, 0, descriptors, null);
            if (outputFrame) {
                frame.getFeatures().add(feature);
                result.add(frame);
            } else {
                result.add(feature);
            }
        }
        return result;
    }
}
