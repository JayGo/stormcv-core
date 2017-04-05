package edu.fudan.stormcv.capture;

import edu.fudan.stormcv.codec.ImageCodec;
import edu.fudan.stormcv.codec.OpenCVImageCodec;
import edu.fudan.stormcv.codec.TurboImageCodec;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;


public class MatToBufferedImageWorker extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(MatToBufferedImageWorker.class);
    private MatPackQueue mMatPackQueue;
    private BufferedImagePackQueue mBufferedImagePackQueue;

    public MatToBufferedImageWorker() {
        mMatPackQueue = MatPackQueue.getInstance();
        mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
    }

    @Override
    public void run() {
        while (true) {
            MatPackQueue.MatPack mMatPack = mMatPackQueue.pop();
            if (mMatPack == null) {
                continue;
            }
//			System.out.println("Transform Thread" + Thread.currentThread().getId() + "is solving Frame " + mMatPack.getSeq());
            if (mMatPack.isLastPack()) {
                //BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
                BufferedImage mBufferedImage = OpenCVImageCodec.getInstance().MatToBufferedImage(mMatPack.getImage(), Frame.JPG_IMAGE);
                while (!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
                    ;
                }
                logger.info("Transform Thread" + Thread.currentThread().getId() + "is exiting");
                break;
            }
            BufferedImage mBufferedImage = OpenCVImageCodec.getInstance().MatToBufferedImage(mMatPack.getImage(), Frame.JPG_IMAGE);
            //BufferedImage mBufferedImage = this.codec.JPEGBytesToBufferedImage(this.codec.MatToJPEGBytes(mMatPack.getImage()));
            //BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
            while (!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
                ;
            }
        }
    }

}
