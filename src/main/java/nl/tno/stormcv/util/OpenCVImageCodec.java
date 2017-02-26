package nl.tno.stormcv.util;

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
public class OpenCVImageCodec {

    public static byte[] MatToBytes(Mat mat, String imageType) {
        MatOfByte buffer = new MatOfByte();
        Highgui.imencode(imageType, mat, buffer);
        return buffer.toArray();
    }

    public static Mat bufferedImageToMat(BufferedImage in) {
        Mat out;
        if (in.getType() == BufferedImage.TYPE_3BYTE_BGR) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
        }
        else if (in.getType() == BufferedImage.TYPE_4BYTE_ABGR
                || in.getType() == BufferedImage.TYPE_4BYTE_ABGR_PRE) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC4);
        }
        else if (in.getType() == BufferedImage.TYPE_BYTE_GRAY) {
            out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
        }
        else {
            return null;
        }
        byte[] data = ((DataBufferByte) in.getRaster().getDataBuffer())
                .getData();
        out.put(0, 0, data);
        return out;
    }

    public static BufferedImage matToBufferedImage(Mat in) {
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
}
