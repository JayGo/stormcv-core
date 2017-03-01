package edu.fudan.lwang.codec;

import edu.fudan.stormcv.model.Frame;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Queue<E> {
    private static Logger logger = LoggerFactory.getLogger(Queue.class);
    public static final int DEFAUTL_MAT_LIMIT = 150;
    public static final int DEFAULT_FRAME_LIMIT = 1600;
    private int limit;
    private int size = 0;
    private String queueId;

    private int dropCount = 0;

    private BlockingQueue<E> queue = new LinkedBlockingDeque<E>();

    public Queue(String quueId) {
        this(quueId, DEFAUTL_MAT_LIMIT);
    }

    public Queue(String queueId, int limit) {
        this.queueId = queueId;
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

//    public int getSize() {
//        return size;
//    }
    public int getSize() {
        return queue.size();
    }

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public void enqueue(E input) {
        //assert (size == queue.size());
//        logger.info(queueId + "Queue size: "+size + ", BlockingQueue size: "+queue.size());
//        logger.info(queueId + " BlockingQueue size: "+queue.size());
        //if (size == limit) {
        if (queue.size() == limit) {
                try {
                    if (queue.take() != null) {
                        if(input instanceof Mat) {
                            logger.info("{} queue drop Mat count: {}", queueId, dropCount);
                        }

                        if(input instanceof Frame) {
                            logger.info("{} queue drop Frame count: {}", queueId, dropCount);
                        }
                        //--size;
//                        if (size < 0) {
//                            logger.warn("{} enqueue error: size={}, queue_size={}", queueId, size, queue.size());
//                        }
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }


        if (queue.offer(input)) {
            //++size;
//            if (size != queue.size()) {
//                logger.error("{} enqueue error: size={}, queue_size={}", queueId, size, queue.size());
//            }
        }
    }

    public E dequeue() {
        E result = null;
        result = queue.poll();

        if (result != null) {
//            --size;
//            if (size < 0) {
//                logger.warn("{} dequeue error: size={}, queue_size={}", queueId, size, queue.size());
//            }
            dropCount = 0;
        } else {
             //logger.info("{} queue size {} : The queue has no element!", queueId, size);
        }
        return result;
    }

    public void clearAll() {
        queue.clear();
    }

    protected String tellQueue() {return "";}
}
