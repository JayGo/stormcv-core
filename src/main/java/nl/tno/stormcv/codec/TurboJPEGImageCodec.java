package nl.tno.stormcv.codec;

import nl.tno.stormcv.constant.GlobalConstants;
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

    private static TurboJPEGImageCodec mTurboJPEGImageCodec;

    private TurboJPEGImageCodec() {
        quality = GlobalConstants.JPEGCodecQuality;
        try {
            compressor = new TJCompressor();
            decompressor = new TJDecompressor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static TurboJPEGImageCodec getInstance() {
        if(mTurboJPEGImageCodec == null) {
            mTurboJPEGImageCodec = new TurboJPEGImageCodec();
        }
        return mTurboJPEGImageCodec;
    }

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
            image = decompressor.decompress(decompressor.getWidth(), decompressor.getHeight(),
                    BufferedImage.TYPE_INT_RGB, 0);
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
            mat = new Mat(decompressor.getHeight(), decompressor.getWidth(), CvType.CV_8UC3);
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
        mat.get(0,0,buffer);
        try {
            compressor.setSourceImage(buffer, 0, 0, mat.width(), (int) (mat.width() * mat.elemSize()), mat.height(), TJ.PF_BGR);
            compressor.setSubsamp(TJ.SAMP_420);
            compressor.setJPEGQuality(90);
            buffer = compressor.compress(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer;
    }
}
