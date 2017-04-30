package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.codec.TurboImageCodec;
import edu.fudan.stormcv.constant.BoltHandleType;
import edu.fudan.stormcv.constant.BoltOperationType;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.ImageHandle;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.FileUtil;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import edu.fudan.stormcv.util.OpertionHandlerFactory;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/2/17 - 6:39 AM
 * Description:
 */
public class ImageFetchAndOperation implements  ISingleInputOperation<CVParticle> {

    private static final Logger logger = LoggerFactory.getLogger(ImageFetchAndOperation.class);
    private ISingleInputOperation operation = null;
    private ConnectorHolder connectorHolder;
    private FileConnector fileConnector = null;
    private ImageCodec codec;
    private BoltHandleType type;
    private  CVParticleSerializer serializer = new FrameSerializer();
    private Map<String, String> streamToOutputLocation = null;
    private boolean drawFrame = true;
    private DrawFeaturesOp drawFeaturesOp;
    private OpertionFactory opertionFactory;
    private int taskIndex = 0;

    public ImageFetchAndOperation() {
        this.streamToOutputLocation = new HashMap<>();
    }

    public ImageFetchAndOperation(ISingleInputOperation operation, BoltHandleType type) {
        this.operation = operation;
        this.type = type;
        this.streamToOutputLocation = new HashMap<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        connectorHolder = new ConnectorHolder(stormConf);
        this.codec = new TurboImageCodec();
//        operation.prepare(stormConf, context);
        if (drawFrame) {
            drawFeaturesOp = new DrawFeaturesOp();
            drawFeaturesOp.prepare(stormConf, context);
        }
        opertionFactory = new OpertionFactory();
        opertionFactory.prepare(stormConf, context);
        if (operation != null) {
            operation.prepare(stormConf, context);
        }
        this.taskIndex = context.getThisTaskIndex();
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<CVParticle> execute(CVParticle input, OperationHandler operationHandler) throws Exception {
        ImageHandle imageHandle = (ImageHandle) input;
        int count = imageHandle.getLocationSize();
        long startTime;
        long endTime;
        List<CVParticle> results = new ArrayList<>();

        if (count > 0) {
            List<String> locations = imageHandle.getLocations();
            BoltOperationType opType = imageHandle.getType();
            if (opType != BoltOperationType.CUSTOM) {
                operation = opertionFactory.getOperation(opType);
                type = opertionFactory.getOperationHandleType(opType);
            } else if (this.operation == null) {
                logger.error("You didn't assign a specific opertion for image processing");
            }

            for (int i = 0; i < count; i++) {
                String location = locations.get(i);
                logger.info("start to process location {}", location);
                fileConnector = connectorHolder.getConnector(location);
                fileConnector.moveTo(location);
                BufferedImage image = ImageIO.read(fileConnector.getAsFile());
                String imageType = location.substring(location.lastIndexOf(".")+1);

                byte[] imageBytes = this.codec.BufferedImageToBytes(image, imageType);
                Frame frame = new Frame(imageHandle.getStreamId(), imageHandle.getSequenceNr(),
                        imageType, imageBytes, 0,
                        new Rectangle(0, 0, image.getWidth(), image.getHeight()));

                if (operation != null) {
                    startTime = System.currentTimeMillis();
                    List<CVParticle> operatedResult = operation.execute(frame, OpertionHandlerFactory.create(type));

                    for (CVParticle particle : operatedResult) {
                        particle.getMetadata().put("location", location);
                        endTime = System.currentTimeMillis();
                        particle.getMetadata().put("endTime", endTime);
                        particle.getMetadata().put("processTime", (endTime - startTime));
                        if (drawFrame) {
                            if (this.streamToOutputLocation.get(input.getStreamId()) == null) {
                                String outputLocation = (String) input.getMetadata().get("outputLocation");

                                outputLocation += "-" + this.taskIndex;
                                FileUtil.TestAndMkdir(outputLocation, false);

                                streamToOutputLocation.put(input.getStreamId(), outputLocation);
                                drawFeaturesOp = drawFeaturesOp.destination(outputLocation);
                            }
                            drawFeaturesOp.execute(particle, OpertionHandlerFactory.create(BoltHandleType.BOLT_HANDLE_TYPE_BUFFEREDIMAGE));
                        }
                    }
                    results.addAll(operatedResult);
                }
            }
        }
        return results;
    }

    @Override
    public void deactivate() {

    }

    @Override
    public CVParticleSerializer<CVParticle> getSerializer() {
        return this.serializer;
    }

    private class OpertionFactory {
        private Map<BoltOperationType, ISingleInputOperation> operationHolder;
        private Map<BoltOperationType, BoltHandleType> operationHandleType;

        public OpertionFactory() {
            operationHolder = new HashMap<>();
            operationHandleType = new HashMap<>();
            registerOperation(BoltOperationType.FACEDETECT,
                    new HaarCascadeOp("faceDetect", GlobalConstants.HaarCacascadeXMLFileName).useMat(true),
                    BoltHandleType.BOLT_HANDLE_TYPE_MAT);
            registerOperation(BoltOperationType.GRAY, new GrayImageOp(),
                    BoltHandleType.BOLT_HANDLE_TYPE_MAT);
            registerOperation(BoltOperationType.SCALE, new ScaleImageOp(0.5f).useMat(false),
                    BoltHandleType.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
            registerOperation(BoltOperationType.COLORHISTOGRAM, new ColorHistogramOp("colorHistogram").useMat(true),
                    BoltHandleType.BOLT_HANDLE_TYPE_MAT);
        }

        public void prepare(Map stormConf, TopologyContext context) throws Exception  {
            for (ISingleInputOperation operation : operationHolder.values()) {
                operation.prepare(stormConf, context);
            }
        }

        public void registerOperation(BoltOperationType type, ISingleInputOperation operation, BoltHandleType handle_type) {
            operationHolder.put(type, operation);
            operationHandleType.put(type, handle_type);
        }

        public ISingleInputOperation getOperation(BoltOperationType type) {
            if (operationHolder.get(type) == null) {
                logger.error("Unsupported operation type");
            }
            return operationHolder.get(type);
        }

        public BoltHandleType getOperationHandleType(BoltOperationType type) {
            if (operationHandleType.get(type) == null) {
                logger.error("Unsupported operation type");
            }
            return operationHandleType.get(type);
        }
    }
}
