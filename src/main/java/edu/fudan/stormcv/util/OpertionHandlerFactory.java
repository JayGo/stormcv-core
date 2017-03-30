package edu.fudan.stormcv.util;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.codec.JPEGImageCodec;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.codec.TurboJPEGImageCodec;
import edu.fudan.stormcv.model.Frame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 
                    
                    private Logger logger = LoggerFactory.getLogger(OperationHandler.class);
                    
                    
                    @Override
                    public boolean fillSourceBufferQueue(Frame frame) {
                    	
                    	////////// decode to Mat //////////
                        this.decodedData = codec.JPEGBytesToMat(frame.getImageBytes());
                        if(decodedData == null || decodedData.empty()) return false;
                        ///////// end of decode ///////
                        
                        return true;
                    }

                    @Override
                    public Mat getDecodedData() {
                        return this.decodedData;
                    }

                    @Override
                    public byte[] getEncodedData(Mat processedResult) {
//                    	long start = System.currentTimeMillis();
                    	byte [] encodedData = codec.MatToJPEGBytes(processedResult);
                    	if(encodedData ==null) return null;
//                    	long end = System.currentTimeMillis();
//                    	

                        return encodedData;
                    }
                };
            }
            // ------------- other types add blow ---------------
            default:
                return null;
        }
    }
}
