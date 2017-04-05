package edu.fudan.stormcv.testcase;

import com.xuggle.xuggler.demos.VideoImage;
import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.codec.OpenCVImageCodec;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.util.LibLoader;
import edu.fudan.stormcv.codec.TurboImageCodec;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Created by jkyan on 1/26/16. This is a test class. No need to concentrate on  it.
 */
public class OpenCVRTSPReaderTest {

    private static final Logger logger = LoggerFactory.getLogger(OpenCVRTSPReaderTest.class);

    public static void main(String[] args) throws IOException {
        LibLoader.loadOpenCVLib();
        String rtspUrl = GlobalConstants.PseudoRtspAddress;
        VideoCapture capture = new VideoCapture(rtspUrl);
        Mat image = new Mat();
        capture.open(rtspUrl);
        int i = 0;
        int width = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
        int height = (int) capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
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
                    BufferedImage bufferedImage = OpenCVImageCodec.getInstance().MatToBufferedImage(image, Frame.JPG_IMAGE);
//                     BufferedImage bufferedImage = ImageUtils.matToBufferedImage(image);
                    videoImage.setImage(bufferedImage);
//                     File file = new File("image"+i+".jpg");
//                     ImageIO.write(bufferedImage, "jpg", file);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Null Pointer Exception during Read");
                }
                i = i + 1;
            }
        } else {
            logger.info("Camera can not be opened!");
        }
        capture.release();
        logger.info("VideoCapture is released");
        System.exit(0);
    }
}
