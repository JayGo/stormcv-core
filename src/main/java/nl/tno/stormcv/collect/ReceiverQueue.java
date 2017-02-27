//package nl.tno.stormcv.collect;
//
//import nl.tno.stormcv.model.Frame;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class ReceiverQueue {
//
//    private List<Frame> frameList;
//    private int maxQueueSize = 100;
//    private long currentFrame = -1;
//
//    private static ReceiverQueue mReceiverQueue;
//
//    private ReceiverQueue() {
//        frameList = new ArrayList<Frame>();
//    }
//
//    public ReceiverQueue setMaxQueueSize(int maxSize) {
//        this.maxQueueSize = maxSize;
//        return mReceiverQueue;
//    }
//
//    public static synchronized ReceiverQueue getInstance() {
//        if (mReceiverQueue == null) {
//            mReceiverQueue = new ReceiverQueue();
//        }
//        return mReceiverQueue;
//    }
//
//    public synchronized boolean push(Frame frame) {
//        if (frameList.size() >= maxQueueSize) {
//            return false;
//        }
//        if (currentFrame == -1) {
//            currentFrame = frame.getSequenceNr();
//        }
////		currentFrame++;
//        frameList.add(frame);
//        return true;
//    }
//
//    /**
//     * pop the first frame in the queue
//     *
//     * @return first frame in the queue OR null if didn't find it
//     */
//    public synchronized Frame pop() {
//        if (frameList.size() > 0) {
//            return frameList.remove(0);
//        }
//        return null;
//    }
//
//    /**
//     * @param frame
//     * @return
//     */
//    public synchronized Frame pop(int frame) {
//        for (int i = 0; i < frameList.size(); i++) {
//            if (frame == frameList.get(i).getSequenceNr()) {
//                return frameList.remove(i);
//            }
//        }
//        return null;
//    }
//
//    /**
//     * pop the frame by frame sequence
//     *
//     * @return next frame in sequence OR null if didn't find it
//     */
//    public synchronized Frame popNext() {
//        for (int i = 0; i < frameList.size(); i++) {
//            if (currentFrame == frameList.get(i).getSequenceNr()) {
//                currentFrame++;
//                return frameList.remove(i);
//            }
//        }
//        //if the frame needed didn't show in the queue when the queue is full, we ignore this frame
//        if (frameList.size() == maxQueueSize) {
//            currentFrame++;
//        }
//        return null;
//    }
//
//
//}
