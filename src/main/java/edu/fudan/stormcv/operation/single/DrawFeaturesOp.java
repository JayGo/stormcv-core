package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Descriptor;
import edu.fudan.stormcv.model.Feature;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.ImageUtils;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import edu.fudan.stormcv.util.connector.LocalFileConnector;
import org.apache.storm.task.TopologyContext;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Point;
import org.opencv.highgui.Highgui;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
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
            Mat matImage = (Mat) codecHandler.getDecodedData();
            if (matImage != null) {
                int colorIndex = 0;
                for (Feature feature : frame.getFeatures()) {
                    Core.putText(matImage, feature.getName(), new Point(10,10), FONT_HERSHEY_SIMPLEX, 1.0, ImageUtils.scalars[0]);
                    for (Descriptor descr : feature.getSparseDescriptors()) {
                        Rectangle box = descr.getBoundingBox().getBounds();
                        Core.rectangle(matImage, new Point(box.x, box.y), new Point(box.x+box.getWidth(), box.y+box.getHeight()),
                                ImageUtils.scalars[colorIndex % ImageUtils.scalars.length]);
                        colorIndex++;
                    }
                }
            }
            frame.swapImageBytes(codecHandler.getEncodedData(matImage));
            if (writeLocation != null) {
                writePicture("jpg", matImage, frame);
            }
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
            if (writeLocation != null) {
                writePicture("jpg", image, frame);
            }
        }

        return result;
    }

    private void writePicture(String writeImageType, Object image, Frame frame) throws IOException {
        Long timeStamp = System.currentTimeMillis();
        String destination = writeLocation + (writeLocation.endsWith("/") ? "" : "/")
                + frame.getStreamId() + "_" + timeStamp + "." + writeImageType;
        logger.info("[drawFeature] destination:{}", destination);
        FileConnector fl = connectorHolder.getConnector(destination);

        if (fl != null) {
            fl.moveTo(destination);
            if (fl instanceof LocalFileConnector) {
                if (useMat) {
                    Mat matImage = (Mat) image;
                    Highgui.imwrite(destination, matImage);
                } else {
                    BufferedImage bufferedImage = (BufferedImage) image;
                    ImageIO.write(bufferedImage, writeImageType, fl.getAsFile());
                }
            } else {
                File tmpImage = File.createTempFile("" + destination.hashCode(), "." + writeImageType);
                if (useMat) {
                    Mat matImage = (Mat) image;
                    Highgui.imwrite(tmpImage.getAbsolutePath(), matImage);
                } else {
                    BufferedImage bufferedImage = (BufferedImage) image;
                    ImageIO.write(bufferedImage, writeImageType, tmpImage);
                }

                boolean flag = fl.copyFile(tmpImage, true);
                if (flag) successCount++;
                else failedCount++;
                logger.info("[drawFeature][success:{}][failed:{}]DrawFeaturesOp Blot saved picture in ",
                        successCount, failedCount, destination);
            }
        }
    }


}
