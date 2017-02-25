package nl.tno.stormcv.codec;

import org.opencv.core.Mat;

import java.awt.image.BufferedImage;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:56 AM
 * Description:
 */
public interface JPEGImageCodec {
    byte[] BufferedImageToJPEGBytes(BufferedImage image);
    BufferedImage JPEGBytesToBufferedImage(byte[] bytes);
    Mat JPEGBytesToMat(byte[] bytes);
    byte[] MatToJPEGBytes(Mat mat);
}
