package nl.tno.stormcv.model;

import org.apache.storm.tuple.Tuple;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CVParticle} object representing a frame from a video stream (or part of a frame called a Tile). Additional to the fields inherited from
 * {@link CVParticle} the Frame object has the following fields:
 * <ul>
 * <li>timeStamp: relative timestamp in ms of this frame with respect to the start of the stream (which is 0 ms)</li>
 * <li>image: the actual frame represented as BufferedImage <i>(may be NULL)</i></li>
 * <li>boundingBox: indicates which part of the original frame this object represents. This might be the full frame but can also be
 * some other region of interest. This box can be used for tiling and combining purposes as well.</li>
 * <li>features: list with {@link Feature} objects (containing one or more {@link Descriptor}) describing
 * aspects of the frame such as SIFT, color histogram or Optical FLow (list may be empty)</li>
 * </ul>
 * <p>
 * A Frame can simply function as a container for multiple Features and hence may not actually have an image. In this case getImageType will
 * return Frame.NO_IMAGE and getImage will return null.
 *
 * @author Corne Versloot
 */
public class Frame extends CVParticle {

    public final static String NO_IMAGE = "no";
    public final static String JPG_IMAGE = "jpg";
    public final static String X264_IMAGE = "x264"; // add by jiu on 2016/12/12

    private long timeStamp;
    private String imageType = JPG_IMAGE;
    private byte[] imageBytes = null;
    private Rectangle boundingBox = null;
    private List<Feature> features = new ArrayList<>();

    public Frame(byte[] imageBytes) {
        this.imageBytes = imageBytes;
    }

    public Frame(String streamId, long sequenceNr, String imageType, byte[] image, long timeStamp, Rectangle boundingBox) {
        super(streamId, sequenceNr);
        this.imageType = imageType;
        this.imageBytes = image;
        this.timeStamp = timeStamp;
        this.boundingBox = boundingBox;
    }

    public Frame(String streamId, long sequenceNr, String imageType, byte[] imageBytes, long timeStamp, Rectangle boundingBox, List<Feature> features) {
        this(streamId, sequenceNr, imageType, imageBytes, timeStamp, boundingBox);
        if (features != null) this.features = features;
    }

    public Frame(Tuple tuple, String imageType, byte[] image, long timeStamp, Rectangle box) {
        super(tuple);
        this.imageType = imageType;
        this.imageBytes = image;
        this.timeStamp = timeStamp;
        this.boundingBox = box;
    }

    public Rectangle getBoundingBox() {
        return boundingBox;
    }

    public void swapImageBytes(byte[] imageBytes) {
        this.imageBytes = imageBytes;
    }

    public long getTimestamp() {
        return this.timeStamp;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    public String getImageType() {
        return imageType;
    }

    public void setImageBytes(byte[] imageBytes, int width, int height) {
        swapImageBytes(imageBytes);
        if (width != this.boundingBox.getWidth() ||
                height != this.boundingBox.getHeight()) {
            this.boundingBox.setSize(width, height);
        }
    }

    public byte[] getImageBytes() {
        return imageBytes;
    }

    @Override
    public String toString() {
        String result = "Frame : {streamId:" + getStreamId() + ", sequenceNr:" + getSequenceNr() + ", timestamp:" + getTimestamp() + ", imageType:" + imageType + ", features:[ ";
        for (Feature f : features) result += f.getName() + " = " + f.getSparseDescriptors().size() + ", ";
        return result + "] }";
    }
}

