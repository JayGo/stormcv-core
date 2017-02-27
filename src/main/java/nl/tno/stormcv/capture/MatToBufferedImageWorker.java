package nl.tno.stormcv.capture;

import nl.tno.stormcv.capture.MatPackQueue.MatPack;
import nl.tno.stormcv.codec.JPEGImageCodec;
import nl.tno.stormcv.codec.TurboJPEGImageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;


public class MatToBufferedImageWorker extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(MatToBufferedImageWorker.class);
    private MatPackQueue mMatPackQueue;
    private BufferedImagePackQueue mBufferedImagePackQueue;
    private JPEGImageCodec codec;

    public MatToBufferedImageWorker() {
        mMatPackQueue = MatPackQueue.getInstance();
        mBufferedImagePackQueue = BufferedImagePackQueue.getInstance();
        this.codec = new TurboJPEGImageCodec();
    }

    @Override
    public void run() {
        while (true) {
            MatPack mMatPack = mMatPackQueue.pop();
            if (mMatPack == null) {
                continue;
            }
//			System.out.println("Transform Thread" + Thread.currentThread().getId() + "is solving Frame " + mMatPack.getSeq());
            if (mMatPack.isLastPack()) {
                //BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
                BufferedImage mBufferedImage = this.codec.JPEGBytesToBufferedImage(this.codec.MatToJPEGBytes(mMatPack.getImage()));
                while (!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
                    ;
                }
                logger.info("Transform Thread" + Thread.currentThread().getId() + "is exiting");
                break;
            }
            BufferedImage mBufferedImage = this.codec.JPEGBytesToBufferedImage(this.codec.MatToJPEGBytes(mMatPack.getImage()));
            //BufferedImage mBufferedImage = ImageUtils.matToBufferedImage(mMatPack.getImage());
            while (!mBufferedImagePackQueue.push(mMatPack.getSeq(), mBufferedImage, mMatPack.isLastPack())) {
                ;
            }
        }
    }

}
