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
public class OpenCVImageCodec implements ImageCodec {

    private static OpenCVImageCodec mOpenCVImageCodec;

    private OpenCVImageCodec() {

    }

    public synchronized static OpenCVImageCodec getInstance() {
        if (mOpenCVImageCodec == null) {
            mOpenCVImageCodec = new OpenCVImageCodec();
        }
        return mOpenCVImageCodec;
    }

    @Override
    public byte[] BufferedImageToBytes(BufferedImage image, String byteType) {
        Mat mat = BufferedImageToMat(image);
        return MatToBytes(mat, byteType);
    }

    @Override
    public BufferedImage BytesToBufferedImage(byte[] bytes, String byteType) {
        Mat in = BytesToMat(bytes, byteType);
        return MatToBufferedImage(in, byteType);
    }

    @Override
    public Mat BytesToMat(byte[] bytes, String byteType) {
        MatOfByte mob = new MatOfByte(bytes);
        return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
    }

    @Override
    public byte[] MatToBytes(Mat mat, String byteType) {
        MatOfByte buffer = new MatOfByte();
        Highgui.imencode("."+byteType, mat, buffer);
        return buffer.toArray();
    }

    public Mat BufferedImageToMat(BufferedImage in) {
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

    public BufferedImage MatToBufferedImage(Mat in, String byteType) {
        MatOfByte byteMat = new MatOfByte();
        Highgui.imencode("."+byteType, in, byteMat);
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

    public byte[] MatToRawBytes(Mat in) {
        byte[] out = new byte[in.width() * in.height() * (int) in.elemSize()];
        in.get(0, 0, out);
        return out;
    }

    public Mat RawBytesToMat(byte[] in, int width, int height, int type) {
        Mat out = Mat.eye(height, width, type);
        out.put(0, 0, in);
        return out;
    }
}
