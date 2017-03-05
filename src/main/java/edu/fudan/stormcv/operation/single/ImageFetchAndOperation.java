package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.codec.JPEGImageCodec;
import edu.fudan.stormcv.codec.TurboJPEGImageCodec;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.ImageHandle;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import edu.fudan.stormcv.util.OpertionHandlerFactory;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.lang.reflect.Constructor;
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
    private ISingleInputOperation operation;
    private ConnectorHolder connectorHolder;
    private FileConnector fileConnector = null;
    private JPEGImageCodec codec;
    private BOLT_HANDLE_TYPE type;
    private  CVParticleSerializer serializer = new FrameSerializer();
    private Map<String, String> streamToOutputLocation = null;
    private boolean drawFrame = true;
    private DrawFeaturesOp drawFeaturesOp;
    private OpertionFactory opertionFactory;

    public ImageFetchAndOperation() {
        this.streamToOutputLocation = new HashMap<>();
    }

    public ImageFetchAndOperation(ISingleInputOperation operation, BOLT_HANDLE_TYPE type) {
        this.operation = operation;
        this.type = type;
        this.streamToOutputLocation = new HashMap<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        connectorHolder = new ConnectorHolder(stormConf);
        this.codec = new TurboJPEGImageCodec();
//        operation.prepare(stormConf, context);
        if (drawFrame) {
            drawFeaturesOp = new DrawFeaturesOp();
            drawFeaturesOp.prepare(stormConf, context);
        }
        opertionFactory = new OpertionFactory();
        opertionFactory.prepare(stormConf, context);
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
            BOLT_OPERTION_TYPE opType = imageHandle.getType();
            operation = opertionFactory.getOperation(opType);
            type = opertionFactory.getOperationHandleType(opType);

            for (int i = 0; i < count; i++) {
                String location = locations.get(i);
                logger.info("start to process location {}", location);
                fileConnector = connectorHolder.getConnector(location);
                fileConnector.moveTo(location);
                BufferedImage image = ImageIO.read(fileConnector.getAsFile());

                byte[] imageBytes = this.codec.BufferedImageToJPEGBytes(image);
                Frame frame = new Frame(imageHandle.getStreamId(), imageHandle.getSequenceNr(),
                        Frame.JPG_IMAGE, imageBytes, 0,
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
                                streamToOutputLocation.put(input.getStreamId(), outputLocation);
                                drawFeaturesOp = drawFeaturesOp.destination(outputLocation);
                            }
                            drawFeaturesOp.execute(particle, OpertionHandlerFactory.create(BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE));
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
        private Map<BOLT_OPERTION_TYPE, ISingleInputOperation> operationHolder;
        private Map<BOLT_OPERTION_TYPE, BOLT_HANDLE_TYPE> operationHandleType;

        public OpertionFactory() {
            operationHolder = new HashMap<>();
            operationHandleType = new HashMap<>();
            registerOperation(BOLT_OPERTION_TYPE.FACEDETECT,
                    new HaarCascadeOp("faceDetect", GlobalConstants.HaarCacascadeXMLFileName).useMat(true),
                    BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
            registerOperation(BOLT_OPERTION_TYPE.GRAY, new GrayImageOp(),
                    BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
            registerOperation(BOLT_OPERTION_TYPE.SCALE, new ScaleImageOp(0.5f).useMat(false),
                    BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
            registerOperation(BOLT_OPERTION_TYPE.COLORHISTOGRAM, new ColorHistogramOp("colorHistogram").useMat(true),
                    BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }

        public void prepare(Map stormConf, TopologyContext context) throws Exception  {
            for (ISingleInputOperation operation : operationHolder.values()) {
                operation.prepare(stormConf, context);
            }
        }

        public void registerOperation(BOLT_OPERTION_TYPE type, ISingleInputOperation operation, BOLT_HANDLE_TYPE handle_type) {
            operationHolder.put(type, operation);
            operationHandleType.put(type, handle_type);
        }

        public ISingleInputOperation getOperation(BOLT_OPERTION_TYPE type) {
            if (operationHolder.get(type) == null) {
                logger.error("Unsupported operation type");
            }
            return operationHolder.get(type);
        }

        public BOLT_HANDLE_TYPE getOperationHandleType(BOLT_OPERTION_TYPE type) {
            if (operationHandleType.get(type) == null) {
                logger.error("Unsupported operation type");
            }
            return operationHandleType.get(type);
        }
    }
}
