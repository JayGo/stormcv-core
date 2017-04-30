package edu.fudan.stormcv.util;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.constant.BoltHandleType;
import edu.fudan.stormcv.codec.TurboImageCodec;
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
    public static OperationHandler<?> create(BoltHandleType type) {
        switch (type) {
            case BOLT_HANDLE_TYPE_BUFFEREDIMAGE: {
                return new OperationHandler<BufferedImage>() {
                    private BufferedImage decodedData = null;

                    private ImageCodec codec = new TurboImageCodec();

                    @Override
                    public boolean fillSourceBufferQueue(Frame frame) {
                        this.decodedData = codec.BytesToBufferedImage(frame.getImageBytes(), frame.getImageType());
                        return true;
                    }

                    @Override
                    public BufferedImage getDecodedData() {
                        return this.decodedData;
                    }

                    @Override
                    public byte[] getEncodedData(BufferedImage processedResult, String imageType) {
                        return codec.BufferedImageToBytes(processedResult, imageType);
                    }
                };
            }
            case BOLT_HANDLE_TYPE_MAT: {
                return new OperationHandler<Mat>() {
                    private Mat decodedData = null;

                    private ImageCodec codec = new TurboImageCodec();

                    @Override
                    public boolean fillSourceBufferQueue(Frame frame) {
                        this.decodedData = codec.BytesToMat(frame.getImageBytes(), frame.getImageType());
                        return true;
                    }

                    @Override
                    public Mat getDecodedData() {
                        return this.decodedData;
                    }

                    @Override
                    public byte[] getEncodedData(Mat processedResult,  String imageType) {
                        return codec.MatToBytes(processedResult, imageType);
                    }
                };
            }
            // ------------- other types add blow ---------------
            default:
                return null;
        }
    }
}
