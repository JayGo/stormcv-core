package edu.fudan.stormcv.util;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.codec.JPEGImageCodec;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.codec.TurboJPEGImageCodec;
import edu.fudan.stormcv.model.Frame;
import org.opencv.core.Mat;

import java.awt.image.BufferedImage;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/26/17 - 5:48 AM
 * Description:
 */
public class OpertionHandlerFactory {
    public static OperationHandler<?> create(BOLT_HANDLE_TYPE type) {
        switch (type) {
            case BOLT_HANDLE_TYPE_BUFFEREDIMAGE: {
                return new OperationHandler<BufferedImage>() {
                    private BufferedImage decodedData = null;

                    private JPEGImageCodec codec = new TurboJPEGImageCodec();

                    @Override
                    public boolean fillSourceBufferQueue(Frame frame) {
                        this.decodedData = codec.JPEGBytesToBufferedImage(frame.getImageBytes());
                        return true;
                    }

                    @Override
                    public BufferedImage getDecodedData() {
                        return this.decodedData;
                    }

                    @Override
                    public byte[] getEncodedData(BufferedImage processedResult) {
                        return codec.BufferedImageToJPEGBytes(processedResult);
                    }
                };
            }
            case BOLT_HANDLE_TYPE_MAT: {
                return new OperationHandler<Mat>() {
                    private Mat decodedData = null;

                    private JPEGImageCodec codec = new TurboJPEGImageCodec();

                    @Override
                    public boolean fillSourceBufferQueue(Frame frame) {
                        this.decodedData = codec.JPEGBytesToMat(frame.getImageBytes());
                        return true;
                    }

                    @Override
                    public Mat getDecodedData() {
                        return this.decodedData;
                    }

                    @Override
                    public byte[] getEncodedData(Mat processedResult) {
                        return codec.MatToJPEGBytes(processedResult);
                    }
                };
            }
            // ------------- other types add blow ---------------
            default:
                return null;
        }
    }
}
