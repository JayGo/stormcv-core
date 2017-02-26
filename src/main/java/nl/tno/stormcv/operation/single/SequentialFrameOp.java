package nl.tno.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FeatureSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An operation that executes a provided set of {@link ISingleInputOperation}&lt;{@link Frame}&gt; sequentially for each
 * Frame it receives. This Operation either results in a {@link Frame} (with or without the image) containing all
 * the features extracted or a list with Features which will be emitted separately once all operations provided their results.
 * Example usage can be chaining of multiple {@link FeatureExtractionOp}'s.
 * <p>
 * The primary usage of this Operation is to minimize network load and overhead caused by duplication of frames to multiple
 * feature extractors in parallel (i.e. in multiple bolts). Using a large number of MultiFeaturesExtractionOperation in parallel
 * is preferred over a large number separate FeatureExtractors in the topology.
 * By default this operation emits all the features calculated separately. This can be changed using the outputFrame and retainImage
 * setters.
 *
 * @author Corne Versloot
 */
@SuppressWarnings("rawtypes")
public class SequentialFrameOp implements ISingleInputOperation<CVParticle> {

    private List<ISingleInputOperation> extractors;
    private CVParticleSerializer serializer = new FeatureSerializer();
    private boolean retainImage = false;
    private boolean outputFrame = false;

    /**
     * Creates a Sequential Feature Extractor Operation which will execute each operation sequentially.
     *
     * @param featureExtractors the set with operations to be executed
     */
    public SequentialFrameOp(List<ISingleInputOperation> featureExtractors) {
        this.extractors = featureExtractors;
    }

    /**
     * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
     * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
     *
     * @param frame
     * @return
     */
    public SequentialFrameOp outputFrame(boolean frame) {
        this.outputFrame = frame;
        if (outputFrame) {
            this.serializer = new FrameSerializer();
        } else {
            this.serializer = new FeatureSerializer();
        }
        return this;
    }

    /**
     * Indicate if the image provided within each Frame received must be added to the output or not. This setting
     * only applies when outputFrame == true. Hence it is possible set the result of this operation to be a
     * {@link Frame} but omit the image by setting retainImage to false. Doing so will lower network usage.
     * Default value is FALSE
     *
     * @param retain
     * @return
     */
    public SequentialFrameOp retainImage(boolean retain) {
        this.retainImage = retain;
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        for (ISingleInputOperation extractor : extractors) {
            extractor.prepare(stormConf, context);
        }
    }

    @Override
    public void deactivate() {
        for (ISingleInputOperation extractor : extractors) {
            extractor.deactivate();
        }
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
        List<CVParticle> result = new ArrayList<>();
        if (!(particle instanceof Frame)) return result;
        Frame frame = (Frame) particle;

        for (ISingleInputOperation extractor : extractors) {
            List<CVParticle> output = extractor.execute(frame, codecHandler);

            if (output.size() == 0) continue;

            if (output.get(0) instanceof Feature) {
                for (CVParticle s : output) frame.getFeatures().add((Feature) s);
            } else if (output.get(0) instanceof Frame) {
                frame = (Frame) output.get(0);
            }
        }

        if (outputFrame) {
            if (!retainImage) {
                frame.swapImageBytes(null);
            }
            result.add(frame);
        } else {
            result.addAll(frame.getFeatures());
        }
        return result;
    }
}
