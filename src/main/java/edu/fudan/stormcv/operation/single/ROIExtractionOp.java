package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Descriptor;
import edu.fudan.stormcv.model.Feature;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Extracts Region's Of Interest from {@link Frame}'s it receives. The ROI's to extract are determined by {@link Feature}'s contained
 * within the frame. If a Feature matches one of the names this operation is instantiated with it will extract a ROI based on each descriptor's bounding box
 * contained within the feature. Each ROI is emitted as a Frame containing the ROI's image. Any Features and Descriptors that apply to the ROI are copied
 * into the Frame and translated into its 'local' coordinate space.
 * <p>
 * Example usecase can be to extract all faces from a video. If this ROIExtractionOperation preceded by a face detector
 * it can extract the detected faces from the frame which can be used for further analysis.
 *
 * @author Corne Versloot
 */
public class ROIExtractionOp implements ISingleInputOperation<Frame> {

    private FrameSerializer serializer = new FrameSerializer();
    private List<String> roisToExtract;
    private int spacing = 0;
    private int minW = 0;
    private int minH = 0;
    private int maxW = Integer.MAX_VALUE;
    private int maxH = Integer.MAX_VALUE;
    private String imageType;

    public ROIExtractionOp(String featureName) {
        this.roisToExtract = new ArrayList<String>();
        roisToExtract.add(featureName);
    }

    public ROIExtractionOp(List<String> featuresToExtract) {
        this.roisToExtract = featuresToExtract;
    }

    public ROIExtractionOp addFeature(String name) {
        this.roisToExtract.add(name);
        return this;
    }

    public ROIExtractionOp spacing(int pixels) {
        this.spacing = pixels;
        return this;
    }

    /**
     * Sets the minimum width of the ROI's to be extracted
     *
     * @param pixels
     * @return
     */
    public ROIExtractionOp minW(int pixels) {
        this.minW = pixels;
        return this;
    }

    /**
     * Sets the minimum height of the ROI's to be extracted
     *
     * @param pixels
     * @return
     */
    public ROIExtractionOp minH(int pixels) {
        this.minH = pixels;
        return this;
    }

    /**
     * Sets the maximum width of ROI's to be extracted
     *
     * @param pixels
     * @return
     */
    public ROIExtractionOp maxW(int pixels) {
        this.maxW = pixels;
        return this;
    }

    /**
     * Sets the maximum height of ROI's to be extracted
     *
     * @param pixels
     * @return
     */
    public ROIExtractionOp maxH(int pixels) {
        this.maxH = pixels;
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        if (stormConf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)) {
            imageType = (String) stormConf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
        }
    }

    @Override
    public void deactivate() {
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }

    private List<Feature> copyFeaturesInROI(Rectangle roi, List<Feature> features) {
        List<Feature> result = new ArrayList<Feature>();
        for (Feature f : features) {
            Feature copyF = f.deepCopy();
            result.add(copyF);

            // copy sparse descriptors
            for (int i = 0; i < copyF.getSparseDescriptors().size(); i++) {
                Descriptor d = copyF.getSparseDescriptors().get(i);
                if (roi.contains(d.getBoundingBox().x, d.getBoundingBox().y)) {
                    // translate descriptors coordinates
                    d.getBoundingBox().x = d.getBoundingBox().x - roi.x;
                    d.getBoundingBox().y = d.getBoundingBox().y - roi.y;
                } else {
                    copyF.getSparseDescriptors().remove(i);
                    i--;
                }
            }
            // TODO: copy dense features?
        }
        return result;
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<Frame> result = new ArrayList<Frame>();
        if (!(particle instanceof Frame)) return result;
        Frame frame = (Frame) particle;

        codecHandler.fillSourceBufferQueue(frame);
        BufferedImage image = (BufferedImage) codecHandler.getDecodedData();

        for (Feature feature : frame.getFeatures()) {
            if (!roisToExtract.contains(feature.getName())) continue;
            for (Descriptor descriptor : feature.getSparseDescriptors()) {
                Rectangle roi = new Rectangle(descriptor.getBoundingBox());

                // check if ROI has the preferred dimensions
                if (roi.getWidth() < minW || roi.getWidth() > maxW || roi.getHeight() < minH || roi.getHeight() > maxH) {
                    continue;
                }
                if (spacing > 0) {
                    roi.x = roi.x - spacing;
                    roi.y = roi.y - spacing;
                    roi.width += 2 * spacing;
                    roi.height += 2 * spacing;
                }
                roi = roi.intersection(frame.getBoundingBox());
                BufferedImage roiImage = null;
                if (image != null) {
                    roiImage = image.getSubimage(roi.x, roi.y, roi.width, roi.height);
                }
//                byte[] buffer = ImageUtils.imageToBytes(roiImage, imageType);
                byte[] buffer = codecHandler.getEncodedData(roiImage, frame.getImageType());

                Frame roiFrame = new Frame(frame.getStreamId() + "_" + result.size(), frame.getSequenceNr(), imageType, buffer, frame.getTimestamp(), roi);
                roiFrame.getFeatures().addAll(copyFeaturesInROI(roi, frame.getFeatures()));
                result.add(roiFrame);
            }
        }
        return result;
    }
}
