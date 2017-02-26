package nl.tno.stormcv.capture;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author lwang
 */
public class BufferedImagePackQueue {

    private static BufferedImagePackQueue mBufferedImageQueue;
    private List<BufferedImagePack> bufferedImagePacks;
    private final int maxQueueSize = 50;

    private BufferedImagePackQueue() {
        bufferedImagePacks = new ArrayList<BufferedImagePack>();
    }

    public static synchronized BufferedImagePackQueue getInstance() {
        if (mBufferedImageQueue == null) {
            mBufferedImageQueue = new BufferedImagePackQueue();
        }
        return mBufferedImageQueue;
    }

    public synchronized boolean push(long seq, BufferedImage image, boolean isLastPack) {
        if (bufferedImagePacks.size() >= maxQueueSize) {
            return false;
        }
        bufferedImagePacks.add(new BufferedImagePack(isLastPack, seq, image));
        return true;
    }

    public synchronized BufferedImagePack pop() {
        if (bufferedImagePacks.size() > 0) {
            return bufferedImagePacks.remove(0);
        }
        return null;
    }

    public synchronized BufferedImagePack pop(long seq) {
        for (int i = 0; i < bufferedImagePacks.size(); i++) {
            BufferedImagePack mBuffedImagePack = bufferedImagePacks.get(i);
            if (mBuffedImagePack.getSeq() == seq + 1) {
                return bufferedImagePacks.remove(i);
            }
        }
        return null;
    }


    public class BufferedImagePack {
        private Date header;
        private long seq;
        private BufferedImage image;
        private boolean isLastPack;

        public BufferedImagePack() {

        }

        public BufferedImagePack(Date header, BufferedImage image) {
            this.header = header;
            this.image = image;
        }

        public BufferedImagePack(long seq, BufferedImage image) {
            this.seq = seq;
            this.image = image;
        }

        public BufferedImagePack(boolean isLastPack, long seq, BufferedImage image) {
            this.seq = seq;
            this.image = image;
            this.isLastPack = isLastPack;
        }

        public long getSeq() {
            return seq;
        }

        public boolean isLastPack() {
            return isLastPack;
        }

        public Date getHeader() {
            return header;
        }

        public BufferedImage getImage() {
            return image;
        }
    }
}
