package edu.fudan.stormcv.model;

import edu.fudan.stormcv.codec.OpenCVJPEGImageCodec;
import org.opencv.core.Mat;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class MatImage {
    private String streamId;
    private Mat mat = null;
    private int width;
    private int height;
    private long timestamp;
    private long sequence;
    private byte[] matbytes = null;
    private int type;

    public MatImage(Mat mat, String streamId, int width, int height,
                    long timestamp, long sequence, int type) {
        this.mat = mat;
        this.streamId = streamId;
        this.width = width;
        this.height = height;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.type = type;
    }

    public MatImage(byte[] matbytes, String streamId, int width, int height,
                    long timestamp, long sequence, int type) {
        // if (matbytes != null) {
        // Mat mat = ImageUtils.bytesToMat(matbytes, width, height,
        // CvType.CV_8UC3);
        // //Mat mat = ImageUtils.bytesToMat(matbytes);
        // this.mat = mat;
        // }
        this.matbytes = matbytes;
        this.streamId = streamId;
        this.width = width;
        this.height = height;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.type = type;
    }

    public edu.fudan.stormcv.model.Frame convertToFrame() throws IOException {
        if (this.mat == null && this.matbytes == null)
            return null;
        edu.fudan.stormcv.model.Frame frame = null;
        BufferedImage image;

//		if (this.mat != null) {
//			image = ImageUtils.matToBufferedImage(this.mat);
//		}
//		else {
//			image = ImageUtils.matBytesToBufferedImage(this.matbytes,
//					this.width, this.height, CvType.CV_8UC3);
//			// this method has a wrong translation of RGB to BGR
//			// image = ImageUtils.bytesToBufferedImage(this.matbytes,
//			// this.width,
//			// this.height, BufferedImage.TYPE_3BYTE_BGR);
//		}

        frame = new edu.fudan.stormcv.model.Frame(this.streamId, this.sequence, "jpg", getMatbytes(),
                this.timestamp, new Rectangle(0, 0, this.width, this.height));
        return frame;
    }

    public void release() {
        mat = null;
        matbytes = null;
    }

    public byte[] getMatbytes() {
        if (matbytes == null && mat != null) {
            matbytes = OpenCVJPEGImageCodec.getInstance().MatToRawBytes(mat);
        }
        return matbytes;
    }

    public void setMatbytes(byte[] matbytes) {
        this.matbytes = matbytes;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Mat getMat() {
        if (mat == null && matbytes != null) {
            mat = OpenCVJPEGImageCodec.getInstance().RawBytesToMat(matbytes, width, height, type);
        }

        return mat;
    }

    public void setMat(Mat mat) {
        this.mat = mat;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
