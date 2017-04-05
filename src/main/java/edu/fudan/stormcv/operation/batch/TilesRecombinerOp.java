package edu.fudan.stormcv.operation.batch;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Feature;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FeatureSerializer;
import edu.fudan.stormcv.codec.TurboImageCodec;
import edu.fudan.stormcv.model.Descriptor;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The TilesRecombinerOperation combines a set of tiles ({@link Frame} objects) originating from the same frame.
 * Tiling can be used to lower the load on image processing steps by decreasing the number of pixels to be analyzed
 * at once. Tiles are Frames which can have {@link Feature}'s and {@link Descriptor}'s. This operation attempts to recombine
 * the tiles back into the original frame with the right features as if the tiling never took place. Hence Features are
 * merged as well and the 'local' locations of descriptors is translated to the 'global' location within the original
 * frame.
 *
 * @author Corne Versloot
 */
public class TilesRecombinerOp implements IBatchOperation<CVParticle> {

    private static final long serialVersionUID = 7348857709867467970L;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private boolean outputFrame = false;
    @SuppressWarnings("rawtypes")
    private CVParticleSerializer serializer = new FeatureSerializer();
    private String imageType;
    private ImageCodec codec = new TurboImageCodec();

    /**
     * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
     * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
     *
     * @param frame
     * @return
     */
    public TilesRecombinerOp outputFrame(boolean frame) {
        this.outputFrame = frame;
        if (outputFrame) {
            this.serializer = new FrameSerializer();
        } else {
            this.serializer = new FeatureSerializer();
        }
        return this;
    }

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
    public CVParticleSerializer<CVParticle> getSerializer() {
        return this.serializer;
    }

    @Override
    public List<CVParticle> execute(List<CVParticle> input) throws Exception {
        Map<String, Feature> featureNameMap = new HashMap<String, Feature>();

        int width = 0, height = 0;
        for (CVParticle particle : input) {
            if (particle instanceof Frame) {
                Frame frame = (Frame) particle;
                width = (int) Math.max(width, frame.getBoundingBox().getMaxX());
                height = (int) Math.max(height, frame.getBoundingBox().getMaxY());
            }
        }

        Rectangle totalFrame = new Rectangle(0, 0, width, height);
        BufferedImage newImage = null;
        if (outputFrame) {
            newImage = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        }
        for (CVParticle particle : input) {
            if (particle instanceof Frame) {
                Frame frame = (Frame) particle;
                Rectangle box = frame.getBoundingBox();
                for (Feature feature : frame.getFeatures()) {
                    merge(feature, featureNameMap, box, totalFrame);
                }
                if (outputFrame) {
                    newImage.getGraphics().drawImage(codec.BytesToBufferedImage(frame.getImageBytes(), frame.getImageType()), box.x, box.y, null);
                }
            } else {
                logger.warn("Can only operate on Frame but got " + particle.getClass().getName() + " else so input is dropped.");
            }
        }

        List<CVParticle> result = new ArrayList<CVParticle>();
        byte[] bytes = codec.BufferedImageToBytes(newImage, imageType);
        if (outputFrame) {
            Frame newFrame = new Frame(input.get(0).getStreamId(), input.get(0).getSequenceNr(),
                    imageType, bytes, ((Frame) input.get(0)).getTimestamp(), totalFrame);
            newFrame.getFeatures().addAll(featureNameMap.values());
            result.add(newFrame);
        } else {
            result.addAll(featureNameMap.values());
        }
        return result;
    }

    @Override
    public List<CVParticle> execute(List<CVParticle> input, OperationHandler operationHandler) throws Exception {
        return null;
    }

    /**
     * Merges the new feature into the set with existing features
     *
     * @param newF
     * @param features
     * @param tile
     */
    private void merge(Feature newF, Map<String, Feature> features, Rectangle tile, Rectangle frame) {
        // first translate descriptors to new location given the boundingBox
        for (Descriptor descriptor : newF.getSparseDescriptors()) {
            descriptor.translate(tile.x, tile.y);
        }

        // add newF to the set with features (possibly merging it with existing one which has the same name)
        Feature feature = features.get(newF.getName());
        if (feature == null) {
            String streamId = newF.getStreamId();
            streamId = streamId.substring(0, streamId.lastIndexOf('_'));

            // add dense descriptor if present
            float[][][] dense;
            if (newF.getDenseDescriptors() != null && newF.getDenseDescriptors().length > 0) {
                float[][][] oldDense = newF.getDenseDescriptors();
                dense = new float[(int) frame.getWidth()][(int) frame.getHeight()][oldDense[0][0].length];
                for (int x = 0; x < oldDense.length; x++) {
                    for (int y = 0; y < oldDense[x].length; y++) {
                        dense[tile.x + x][tile.y + y] = oldDense[x][y];
                    }
                }
            } else {
                dense = null;
            }
            Feature combiFeature = new Feature(streamId, newF.getSequenceNr(), newF.getName(), newF.getDuration(), newF.getSparseDescriptors(), dense);
            features.put(newF.getName(), combiFeature);
        } else {
            feature.getSparseDescriptors().addAll(newF.getSparseDescriptors());
            Map<String, Object> metadata = feature.getMetadata();
            for (String key : newF.getMetadata().keySet())
                if (!metadata.containsKey(key)) {
                    metadata.put(key, newF.getMetadata().get(key));
                }

            // add dense descriptors (if present)
            if (feature.getDenseDescriptors() != null && newF.getDenseDescriptors() != null) {
                float[][][] oldDense = newF.getDenseDescriptors();
                float[][][] dense = feature.getDenseDescriptors();
                for (int x = 0; x < oldDense.length; x++) {
                    for (int y = 0; y < oldDense[x].length; y++) {
                        dense[tile.x + x][tile.y + y] = oldDense[x][y];
                    }
                }
            }
        }
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }
}
