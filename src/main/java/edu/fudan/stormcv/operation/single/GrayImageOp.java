package edu.fudan.stormcv.operation.single;

import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.TimeElasper;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.awt.image.ColorConvertOp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A simple operation that converts the image within received {@link Frame} objects to gray
 * using Java's native {@link ColorConvertOp}. Hence the OpenCV library is not required to use this Operation.
 *
 * @author Corne Versloot
 */
public class GrayImageOp implements ISingleInputOperation<Frame> {

    private Logger logger = LoggerFactory.getLogger(GrayImageOp.class);
    private FrameSerializer serializer = new FrameSerializer();
    private TimeElasper timeElasper = new TimeElasper();
    private int frameNr = 0;
    
    private TimeElasper timeElasperDecoder = new TimeElasper();
    private TimeElasper timeElasperEncoder = new TimeElasper();
    private int frameNrDecoded = 0;
    private int frameNrEncoded = 0;
    
    private TimeElasper sizeElasper = new TimeElasper();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public CVParticleSerializer<Frame> getSerializer() {
        return serializer;
    }

    @Override
    public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<Frame> results = new ArrayList<>();
        Frame frame = (Frame) particle;
        

        
        if (frame.getImageBytes().length <= 0) {
            return results;
        }
        //logger.info("frame {} rectangle:{}, size:{}", frame.getSequenceNr(), frame.getBoundingBox(), frame.getImageBytes().length);

//      	long startDecoding = System.currentTimeMillis();
        codecHandler.fillSourceBufferQueue(frame);
//        long endDecoding = System.currentTimeMillis();
        
//        timeElasperDecoder.push((int)(endDecoding-startDecoding));
//		if(frameNrDecoded == 100 || frameNrDecoded == 500 || frameNrDecoded == 900
//				|| frameNrDecoded == 1300 || frameNrDecoded == 1700 || frameNrDecoded == 2100 || frameNrDecoded == 2500) {
//			logger.info("JPEG Decoder on "+this.getClass().getSimpleName()+": Top "+frameNrDecoded+"'s time average cost: "+timeElasperDecoder.getKAve(frameNrDecoded));
//		}

		
		

        Mat in = (Mat) codecHandler.getDecodedData();

        
//        if(frameNrDecoded == 500) {
//        	Highgui.imwrite("/home/jliu/Pictures/jpegcpu/"+frameNrDecoded+"_codec.jpg", in);
//        }
        
		frameNrDecoded++;
		
        if (in != null) {
//            logger.info("frame size: {}", frame.getImageBytes().length);
            long start = System.currentTimeMillis();
            
        	Mat out = new Mat(in.height(), in.width(), CvType.CV_8UC1);
            Imgproc.cvtColor(in, out, Imgproc.COLOR_BGR2GRAY);
//            logger.info("Mat size: {} x {}", in.width(), in.height());
            
            long end = System.currentTimeMillis();
            
            timeElasper.push((int)(end-start));
            
            sizeElasper.push(frame.getImageBytes().length);
            
    		if(frameNr == 100 || frameNr == 500 || frameNr == 900
    				|| frameNr == 1300 || frameNr == 1700 || frameNr == 2100 || frameNr == 2500) {
    			logger.info("Operation on: "+getContext()+" Top "+frameNr+"'s time average cost: "+timeElasper.getKAve(frameNr));
    			logger.info("Encoding data top {} ave size: {}", frameNr, sizeElasper.getKAve(frameNr));
    		}
    		frameNr++;
    		
//        	long startEncoding = System.currentTimeMillis();
            byte[] encodedData = codecHandler.getEncodedData(out);
//        	long endEncoding = System.currentTimeMillis();
        	
        	
//        	timeElasperEncoder.push((int)(endEncoding-startEncoding));
//    		if(frameNrEncoded == 100 || frameNrEncoded == 500 || frameNrEncoded == 900
//    				|| frameNrEncoded == 1300 || frameNrEncoded == 1700 || frameNrEncoded == 2100 || frameNrEncoded == 2500) {
//    			logger.info("JPEG Encoder on "+this.getClass().getSimpleName()+": Top "+frameNrEncoded+"'s time average cost: "+timeElasperEncoder.getKAve(frameNrEncoded));
//    		}
//    		frameNrEncoded++;
            
            if (encodedData == null) {
                logger.error("encode data is null!!");
            }
            frame.swapImageBytes(encodedData);
            results.add(frame);
        }
        
        
// else {
//            logger.info("cannot decode frame {}", frame.getSequenceNr());
//        }
        return results;
    }

    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }
}
