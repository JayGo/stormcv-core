package edu.fudan.stormcv.codec;

import edu.fudan.stormcv.constant.GlobalConstants;
import org.libjpegturbo.turbojpeg.TJ;
import org.libjpegturbo.turbojpeg.TJCompressor;
import org.libjpegturbo.turbojpeg.TJDecompressor;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/21/17 - 7:15 AM
 * Description:
 */
public class TurboImageCodec implements ImageCodec {
    private TJCompressor compressor;
    private TJDecompressor decompressor;
    private int quality;

    public TurboImageCodec() {
        quality = GlobalConstants.JPEGCodecQuality;
        try {
            compressor = new TJCompressor();
            decompressor = new TJDecompressor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] BufferedImageToBytes(BufferedImage image, String byteType) {
        byte[] buffer = null;
        try {
            if (byteType.equalsIgnoreCase("jpg")) {
                compressor.setSourceImage(image, 0, 0, image.getWidth(), image.getHeight());
                compressor.setSubsamp(TJ.SAMP_420);
                compressor.setJPEGQuality(quality);
                buffer = compressor.compress(0);
            } else {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ImageIO.write(image, byteType, out);
                buffer = out.toByteArray();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer;
    }

    @Override
    public BufferedImage BytesToBufferedImage(byte[] bytes, String byteType) {
        BufferedImage image = null;
        try {
            if (byteType.equalsIgnoreCase("jpg")) {
                decompressor.setSourceImage(bytes, bytes.length);
                image = decompressor.decompress(decompressor.getWidth(), decompressor.getHeight(),
                        BufferedImage.TYPE_3BYTE_BGR, 0);
            } else {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                image = ImageIO.read(bais);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return image;
    }

    @Override
    public Mat BytesToMat(byte[] bytes, String byteType) {
        Mat mat = null;
        int width = 0;
        int height = 0;
        byte[] dstBuffer = null;
        try {
            if (byteType.equalsIgnoreCase("jpg")) {
                decompressor.setSourceImage(bytes, bytes.length);
                width = decompressor.getWidth();
                height = decompressor.getHeight();
                dstBuffer = decompressor.decompress(width, width * TJ.getPixelSize(TJ.PF_BGR), height, TJ.PF_BGR, 0);
            } else {
                BufferedImage bufferedImage = BytesToBufferedImage(bytes, byteType);
                dstBuffer = ((DataBufferByte) bufferedImage.getRaster().getDataBuffer()).getData();
            }
            mat = new Mat(height, width, CvType.CV_8UC3);
            mat.put(0, 0, dstBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mat;
    }

    @Override
    public byte[] MatToBytes(Mat mat, String byteType) {
        byte[] buffer;
        if (byteType.equalsIgnoreCase("jpg")) {
            long size = mat.total() * mat.channels();
            buffer = new byte[(int) size];
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
        } else {
            MatOfByte matOfByte = new MatOfByte();
            Highgui.imencode(byteType, mat, matOfByte);
            buffer = matOfByte.toArray();
        }
        return buffer;
    }
}
