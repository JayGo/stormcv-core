package nl.tno.stormcv.util;

import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.highgui.VideoCapture;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by jkyan on 1/26/16. This is a test class. No need to concentrate on  it.
 */
public class OpenCVRTSPReader {

     public static void main(String[] args) throws IOException {
         //System.load("/lib/linux64_opencv_java248.so");
         NativeUtils.loadOpencvLib();
         //VideoCapture capture = new VideoCapture("rtsp://218.204.223.237:554/live/1/66251FC11353191F/e7ooqwcfbqjoo80j.sdp");
         VideoCapture capture = new VideoCapture("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
         // VideoCapture capture = new VideoCapture("http://10.8.8.105:60002/");

         Mat image=new Mat();
         double propid=0;

         //capture.open("rtsp://218.204.223.237:554/live/1/66251FC11353191F/e7ooqwcfbqjoo80j.sdp");
         capture.open("rtsp://admin:123456qaz@10.134.141.177:554/h264/ch1/main/av_stream");
         // capture.open("http://10.8.8.105:60002/");
         int i = 0;
         int width     = (int)capture.get(Highgui.CV_CAP_PROP_FRAME_WIDTH);
         int height    = (int)capture.get(Highgui.CV_CAP_PROP_FRAME_HEIGHT);
         double frameRate = capture.get(5);    //frameRate = cap.get(Highgui.CV_CAP_PROP_FPS);
         System.out.println("frameRate:" + frameRate);
         System.out.println(width + "x" + height + " at " + frameRate);

         if (capture.isOpened()) {
             System.out.println("Video is captured");

             //VideoImage videoImage = new VideoImage();

             while (true) {

                 try {
                     //capture.retrieve(image); //same results as .read()
                     capture.read(image);
                     frameRate = capture.get(Highgui.CV_CAP_PROP_AUTOGRAB);
                     System.out.println("frameRate:" + frameRate);
                     //Highgui.imwrite("camera"+i+".jpg", image);
                     //BufferedImage bufferedImage = ImageUtils.bytesToBufferedImage(ImageUtils.matToBytes(image), width, height, BufferedImage.TYPE_3BYTE_BGR);
                     BufferedImage bufferedImage = ImageUtils.matToBufferedImage(image);
                     //videoImage.setImage(bufferedImage);
                     File file = new File("image"+i+".jpg");
                     ImageIO.write(bufferedImage, "jpg", file);
                     System.out.println("read a frame...");
                 } catch (Exception e) {
                     e.printStackTrace();
                     System.out.println("Null Pointer Exception during Read");
                 }
                 i=i+1;
             }
         } else {
             System.out.println("Camera can not be opened!");
         }
         capture.release();
         System.out.println("VideoCapture is released");
         System.exit(0);
     }
}
