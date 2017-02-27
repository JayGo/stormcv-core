package edu.fudan.stormcv.codec;

import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/20/17 - 4:15 AM
 * Description:
 */
public class OpenCVJPEGImageCodec implements JPEGImageCodec {

    private static OpenCVJPEGImageCodec mOpenCVJPEGImageCodec;

    private OpenCVJPEGImageCodec() {

    }

    public synchronized static OpenCVJPEGImageCodec getInstance() {
        if (mOpenCVJPEGImageCodec == null) {
            mOpenCVJPEGImageCodec = new OpenCVJPEGImageCodec();
        }
        return mOpenCVJPEGImageCodec;
    }

    @Override
    public byte[] BufferedImageToJPEGBytes(BufferedImage image) {
        Mat mat = bufferedImageToMat(image);
        return MatToJPEGBytes(mat);
    }

    @Override
    public BufferedImage JPEGBytesToBufferedImage(byte[] bytes) {
        Mat in = JPEGBytesToMat(bytes);
        return matToBufferedImage(in);
    }

    @Override
    public Mat JPEGBytesToMat(byte[] bytes) {
        MatOfByte mob = new MatOfByte(bytes);
        return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
    }

    @Override
    public byte[] MatToJPEGBytes(Mat mat) {
        MatOfByte buffer = new MatOfByte();
        Highgui.imencode(".jpg", mat, buffer);
        return buffer.toArray();
    }

    private Mat bufferedImageToMat(BufferedImage in) {
        Mat out;
        if (in.getType() == BufferedImage.TYPE_3BYTE_BGR) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
        } else if (in.getType() == BufferedImage.TYPE_4BYTE_ABGR
                || in.getType() == BufferedImage.TYPE_4BYTE_ABGR_PRE) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC4);
        } else if (in.getType() == BufferedImage.TYPE_BYTE_GRAY) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
        } else {
            return null;
        }
        byte[] data = ((DataBufferByte) in.getRaster().getDataBuffer())
                .getData();
        out.put(0, 0, data);
        return out;
    }

    private static BufferedImage matToBufferedImage(Mat in) {
        MatOfByte byteMat = new MatOfByte();
        Highgui.imencode(".jpg", in, byteMat);
        byte[] bytes = byteMat.toArray();
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        BufferedImage img = null;
        try {
            img = ImageIO.read(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return img;
    }

    public static byte[] MatToRawBytes(Mat in) {
        byte[] out = new byte[in.width() * in.height() * (int) in.elemSize()];
        in.get(0, 0, out);
        return out;
    }

    public static Mat RawBytesToMat(byte[] in, int width, int height, int type) {
        Mat out = Mat.eye(height, width, type);
        out.put(0, 0, in);
        return out;
    }
}
