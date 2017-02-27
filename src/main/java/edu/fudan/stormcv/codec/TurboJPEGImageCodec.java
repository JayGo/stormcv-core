package edu.fudan.stormcv.codec;

import edu.fudan.stormcv.constant.GlobalConstants;
import org.libjpegturbo.turbojpeg.TJ;
import org.libjpegturbo.turbojpeg.TJCompressor;
import org.libjpegturbo.turbojpeg.TJDecompressor;
import org.opencv.core.CvType;
import org.opencv.core.Mat;

import java.awt.image.BufferedImage;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/21/17 - 7:15 AM
 * Description:
 */
public class TurboJPEGImageCodec implements JPEGImageCodec {
    private TJCompressor compressor;
    private TJDecompressor decompressor;
    private int quality;

    //private static TurboJPEGImageCodec mTurboJPEGImageCodec;

    public TurboJPEGImageCodec() {
        quality = GlobalConstants.JPEGCodecQuality;
        try {
            compressor = new TJCompressor();
            decompressor = new TJDecompressor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public synchronized static TurboJPEGImageCodec getInstance() {
//        if(mTurboJPEGImageCodec == null) {
//            mTurboJPEGImageCodec = new TurboJPEGImageCodec();
//        }
//        return mTurboJPEGImageCodec;
//    }

    @Override
    public byte[] BufferedImageToJPEGBytes(BufferedImage image) {
        byte[] buffer = null;
        try {
            compressor.setSourceImage(image, 0, 0, image.getWidth(), image.getHeight());
            compressor.setSubsamp(TJ.SAMP_420);
            compressor.setJPEGQuality(quality);
            buffer = compressor.compress(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer;
    }

    @Override
    public BufferedImage JPEGBytesToBufferedImage(byte[] bytes) {
        BufferedImage image = null;
        try {
            decompressor.setSourceImage(bytes, bytes.length);
            //System.out.println("decompress:" + decompressor.getWidth() +"x" + decompressor.getHeight());
            image = decompressor.decompress(decompressor.getWidth(), decompressor.getHeight(),
                    BufferedImage.TYPE_3BYTE_BGR, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return image;
    }

    @Override
    public Mat JPEGBytesToMat(byte[] bytes) {
        Mat mat = null;
        try {
            decompressor.setSourceImage(bytes, bytes.length);
            int width = decompressor.getWidth();
            int height = decompressor.getHeight();
            byte[] dstBuffer = decompressor.decompress(width, width * TJ.getPixelSize(TJ.PF_BGR), height, TJ.PF_BGR, 0);
            mat = new Mat(height, width, CvType.CV_8UC3);
            mat.put(0, 0, dstBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mat;
    }

    @Override
    public byte[] MatToJPEGBytes(Mat mat) {
        long size = mat.total() * mat.channels();
        byte[] buffer = new byte[(int) size];
        mat.get(0, 0, buffer);
        int fromType;
        int sampType;
        if (mat.channels() == 3) {
            fromType = TJ.PF_BGR;
            sampType = TJ.SAMP_420;
        } else if (mat.channels() == 1) {
            fromType = TJ.PF_GRAY;
            sampType = TJ.SAMP_GRAY;
        } else {
            fromType = -1;
            sampType = -1;
        }

        try {
            compressor.setSourceImage(buffer, 0, 0, mat.width(), (int) (mat.width() * mat.elemSize()), mat.height(), fromType);
            compressor.setSubsamp(sampType);
            compressor.setJPEGQuality(GlobalConstants.JPEGCodecQuality);
            buffer = compressor.compress(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer;
    }
}
