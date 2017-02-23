package nl.tno.stormcv.testcase;

import com.xuggle.xuggler.demos.VideoImage;
import nl.tno.stormcv.constant.GlobalConstants;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.LibLoader;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by jkyan on 1/26/16. This is a test class. No need to concentrate on  it.
 */
public class OpenCVRTSPReader {

    private static final Logger logger = LoggerFactory.getLogger(OpenCVRTSPReader.class);

     public static void main(String[] args) throws IOException {
         LibLoader.loadOpenCVLib();
         String rtspUrl = GlobalConstants.PseudoRtspAddress;
         VideoCapture capture = new VideoCapture(rtspUrl);
         Mat image=new Mat();
         capture.open(rtspUrl);
         int i = 0;
         int width     = (int)capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
         int height    = (int)capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
         double frameRate = capture.get(5);
         logger.info(width + "x" + height + " , frame = " + frameRate);
         if (capture.isOpened()) {
             logger.info("Video is captured");
             VideoImage videoImage = new VideoImage();
             while (true) {
                 try {
//                     capture.retrieve(image); //same results as .read()
                     capture.read(image);
//                     Highgui.imwrite("camera"+i+".jpg", image);
                     BufferedImage bufferedImage = ImageUtils.matToBufferedImage(image);
                     videoImage.setImage(bufferedImage);
//                     File file = new File("image"+i+".jpg");
//                     ImageIO.write(bufferedImage, "jpg", file);
                 } catch (Exception e) {
                     e.printStackTrace();
                     logger.info("Null Pointer Exception during Read");
                 }
                 i=i+1;
             }
         } else {
             logger.info("Camera can not be opened!");
         }
         capture.release();
         logger.info("VideoCapture is released");
         System.exit(0);
     }
}
