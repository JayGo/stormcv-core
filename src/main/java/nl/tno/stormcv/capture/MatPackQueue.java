package nl.tno.stormcv.capture;

import org.opencv.core.Mat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MatPackQueue {

    private static MatPackQueue mMatPackQueue;
    private List<MatPack> matPacks;
    private final int matQueueSize = 50;
    private long currentSeq;

    private MatPackQueue() {
        matPacks = new ArrayList<MatPack>();
        currentSeq = 0;
    }

    public static synchronized MatPackQueue getInstance() {
        if (mMatPackQueue == null) {
            mMatPackQueue = new MatPackQueue();
        }
        return mMatPackQueue;
    }

    public synchronized boolean push(Mat image, boolean isLastPack) {
        if (matPacks.size() >= matQueueSize) {
            return false;
        }
        currentSeq++;
        matPacks.add(new MatPack(currentSeq, image, isLastPack));
        return true;
    }

    public synchronized MatPack pop() {
        if (matPacks.size() > 0) {
            return matPacks.remove(0);
        }
        return null;
    }


    class MatPack {

        private Date header;
        private long seq;
        private boolean isLastPack;
        private Mat image;

        public MatPack() {

        }

        public MatPack(Date timestamp, Mat image) {
            this.header = timestamp;
            this.image = image;
        }

        public MatPack(long seq, Mat image) {
            this.seq = seq;
            this.image = image;
        }

        public MatPack(long seq, Mat image, boolean isLastPack) {
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

        public Mat getImage() {
            return image;
        }
    }

}
