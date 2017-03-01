package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Descriptor;
import edu.fudan.stormcv.model.Feature;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.ImageUtils;
import edu.fudan.stormcv.util.connector.LocalFileConnector;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.*;
import org.opencv.core.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencv.core.Core.FONT_HERSHEY_SIMPLEX;

/**
 * Draws the features contained within a {@link Frame} on the image itself and is primarily used for testing purposes.
 * The bounding boxes are drawn to indicate the location of a {@link Descriptor}. If a <code>writeLocation</code> is provided the image will be written
 * to that location as PNG image with filename: streamID_sequenceNR_randomNR.png.
 *
 * @author Corne Versloot
 */
public class DrawFeaturesOp implements ISingleInputOperation<Frame> {
    private Logger logger = LoggerFactory.getLogger(DrawFeaturesOp.class);
    private FrameSerializer serializer = new FrameSerializer();
    private String writeLocation;
    private ConnectorHolder connectorHolder;
    private boolean drawMetadata = false;
    private static int successCount = 0;
    private static int failedCount = 0;
    private boolean useMat = false;

    public DrawFeaturesOp destination(String location) {
        this.writeLocation = location;
        return this;
    }

    public String name() {
        return "drawer";
    }

    /**
     * Indicates if metadata attached to the Frame must be written in the frame as well
     * (as 'key = value' in the upper left corner)
     *
     * @param bool
     * @return itself
     */
    public DrawFeaturesOp drawMetadata(boolean bool) {
        this.drawMetadata = bool;
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        this.connectorHolder = new ConnectorHolder(stormConf);
    }

    @Override
    public void deactivate() {
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<Frame> result = new ArrayList<>();
        if (!(particle instanceof Frame)) return result;
        Frame frame = (Frame) particle;
        result.add(frame);

        codecHandler.fillSourceBufferQueue(frame);

        if (useMat) {
            Mat image = (Mat) codecHandler.getDecodedData();
            if (image != null) {
                int colorIndex = 0;
                for (Feature feature : frame.getFeatures()) {
                    Core.putText(image, feature.getName(), new Point(10,10), FONT_HERSHEY_SIMPLEX, 1.0, ImageUtils.scalars[0]);
                    for (Descriptor descr : feature.getSparseDescriptors()) {
                        Rectangle box = descr.getBoundingBox().getBounds();
                        Core.rectangle(image, new Point(box.x, box.y), new Point(box.x+box.getWidth(), box.y+box.getHeight()),
                                ImageUtils.scalars[colorIndex % ImageUtils.scalars.length]);
                        colorIndex++;
                    }
                }
            }
            frame.swapImageBytes(codecHandler.getEncodedData(image));
        } else {
            BufferedImage image = (BufferedImage) codecHandler.getDecodedData();
            if (image == null) return result;
            Graphics2D graphics = image.createGraphics();
            int colorIndex = 0;
            for (Feature feature : frame.getFeatures()) {
                graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
                for (Descriptor descr : feature.getSparseDescriptors()) {
                    Rectangle box = descr.getBoundingBox().getBounds();
                    if (box.width == 0) box.width = 1;
                    if (box.height == 0) box.height = 1;
                    graphics.draw(box);
                }
                colorIndex++;
            }

            int y = 10;
            // draw feature legenda on top of everything else
            for (colorIndex = 0; colorIndex < frame.getFeatures().size(); colorIndex++) {
                graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
                graphics.drawString(frame.getFeatures().get(colorIndex).getName(), 5, y);
                y += 12;
            }

            if (drawMetadata) for (String key : frame.getMetadata().keySet()) {
                colorIndex++;
                graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
                graphics.drawString(key + " = " + frame.getMetadata().get(key), 5, y);
                y += 12;
            }
            frame.swapImageBytes(codecHandler.getEncodedData(image));
        }

//        if (writeLocation != null) {
//            String writeImageType = "jpg";
//            Long timeStamp = System.currentTimeMillis();
//            String destination = writeLocation + (writeLocation.endsWith("/") ? "" : "/")
//                    + frame.getStreamId() + "_" + timeStamp + "." + writeImageType;
//            logger.info("[drawFeature] destination:{}", destination);
//            FileConnector fl = connectorHolder.getConnector(destination);
//            if (fl != null) {
//                fl.moveTo(destination);
//                if (fl instanceof LocalFileConnector) {
//                    ImageIO.write(image, writeImageType, fl.getAsFile());
//                } else {
//                    File tmpImage = File.createTempFile("" + destination.hashCode(), "." + writeImageType);
//                    ImageIO.write(image, writeImageType, tmpImage);
//                    boolean flag = fl.copyFile1(tmpImage, true);
//                    if (flag) successCount++;
//                    else failedCount++;
//                    logger.info("[drawFeature][success:{}][failed:{}]DrawFeaturesOp Blot saved picture in ",
//                            successCount, failedCount, destination);
//                }
//            }
//        }
        return result;
    }


}
