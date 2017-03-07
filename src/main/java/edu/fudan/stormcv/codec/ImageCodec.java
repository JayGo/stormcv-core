package edu.fudan.stormcv.codec;

import org.opencv.core.Mat;

import java.awt.image.BufferedImage;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/23/17 - 6:56 AM
 * Description:
 */
public interface ImageCodec {
    byte[] BufferedImageToBytes(BufferedImage image, String byteType);

    BufferedImage BytesToBufferedImage(byte[] bytes, String byteType);

    Mat BytesToMat(byte[] bytes, String byteType);

    byte[] MatToBytes(Mat mat, String byteType);
}
