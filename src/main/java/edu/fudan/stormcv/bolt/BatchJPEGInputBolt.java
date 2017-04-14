package edu.fudan.stormcv.bolt;

import com.google.common.cache.*;
import edu.fudan.lwang.codec.*;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.batcher.IBatcher;
import edu.fudan.stormcv.constant.BOLT_HANDLE_TYPE;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.operation.batch.IBatchOperation;
import edu.fudan.stormcv.operation.single.ISingleInputOperation;
import edu.fudan.stormcv.util.LibLoader;
import edu.fudan.stormcv.util.OpertionHandlerFactory;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A basic {@link CVParticleBolt} implementation that works with single items received (hence maintains no
 * history of items received). The bolt will ask the provided {@link ISingleInputOperation} implementation to
 * work on the input it received and produce zero or more results which will be emitted by the bolt.
 * If an operation throws an exception the input will be failed in all other situations the input will be acked.
 *
 * @author Corne Versloot
 */
public class BatchJPEGInputBolt extends CVParticleBolt implements RemovalListener<CVParticle, String> {

    private final Logger logger = LoggerFactory.getLogger(BatchJPEGInputBolt.class);

    private IBatchOperation<? extends CVParticle> operation;
    private IBatcher batcher;
    private int TTL = 29;
    private int maxSize = 256;
    private Fields groupBy;
    private History history;
    private boolean refreshExperation = true;
    private BOLT_HANDLE_TYPE boltHandleType;

    private long startTime;
    private long endTime;
    private int count;


    /**
     * Constructs a SingleInputOperation
     *
     * @param operation the operation to be performed
     */
    public BatchJPEGInputBolt(IBatcher batcher, IBatchOperation<? extends CVParticle> operation, BOLT_HANDLE_TYPE type) {
        this.operation = operation;
        this.batcher = batcher;
        this.boltHandleType = type;
    }

    /**
     * Sets the time to live for items being cached by this bolt
     *
     * @param ttl
     * @return
     */
    public BatchJPEGInputBolt ttl(int ttl) {
        this.TTL = ttl;
        return this;
    }

    /**
     * Specifies the maximum size of the cashe used. Hitting the maximum will cause oldest items to be
     * expired from the cache
     *
     * @param size
     * @return
     */
    public BatchJPEGInputBolt maxCacheSize(int size) {
        this.maxSize = size;
        return this;
    }

    /**
     * Specifies the fields used to group items on.
     *
     * @param group
     * @return
     */
    public BatchJPEGInputBolt groupBy(Fields group) {
        this.groupBy = group;
        return this;
    }

    /**
     * Specifies weather items with hither sequence number within a group must have their
     * ttl's refreshed if an item with lower sequence number is added
     *
     * @param refresh
     * @return
     */
    public BatchJPEGInputBolt refreshExpiration(boolean refresh) {
        this.refreshExperation = refresh;
        return this;
    }



    @SuppressWarnings("rawtypes")
    @Override
    void prepare(Map stormConf, TopologyContext context) {
        try {
            LibLoader.loadOpenCVLib();
            LibLoader.loadHgCodecLib();
            operation.prepare(stormConf, context);
        } catch (Exception e) {
            logger.error("Unale to prepare Operation ", e);
        }

        // use TTL and maxSize from config if they were not set explicitly using the constructor (implicit way of doing this...)
        if (TTL == 29)
            TTL = stormConf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ?
                    TTL : ((Long) stormConf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC)).intValue();
        if (maxSize == 256)
            maxSize = stormConf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE) == null ?
                    maxSize : ((Long) stormConf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE)).intValue();
        history = new History(this, this.TTL, this.maxSize, this.refreshExperation);

        // IF NO grouping was set THEN select the first grouping registered for the spout as the grouping used within the Spout (usually a good guess)
        if (groupBy == null) {
            Map<GlobalStreamId, Grouping> sources = context.getSources(context.getThisComponentId());
            for (GlobalStreamId id : sources.keySet()) {
                Grouping grouping = sources.get(id);
                this.groupBy = new Fields(grouping.get_fields());
                break;
            }
        }

        // prepare the selector and operation
        try {
            batcher.prepare(stormConf);
//            operation.prepare(stormConf, context);
        } catch (Exception e) {
            logger.error("Unable to preapre the Selector or Operation", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(operation.getSerializer().getFields());
    }

    @Override
    public void execute(Tuple input) {
        String group = generateKey(input);
        if (group == null) {
            collector.fail(input);
            return;
        }
        CVParticle particle;
        try {
            particle = deserialize(input);
            history.add(group, particle);
            List<List<CVParticle>> batches = batcher.partition(history, history.getGroupedItems(group));
            for (List<CVParticle> batch : batches) {
                try {
                    List<? extends CVParticle> results = operation.execute(batch,
                            OpertionHandlerFactory.create(boltHandleType));
                    for (CVParticle result : results) {
                        result.setRequestId(particle.getRequestId());
                        collector.emit(input, serializers.get(result.getClass().getName()).toTuple(result));
                    }
                } catch (Exception e) {
                    logger.warn("Unable to to process batch due to ", e);
                }
            }
        } catch (IOException e1) {
            logger.warn("Unable to deserialize Tuple", e1);
        }
        idleTimestamp = System.currentTimeMillis();
    }

    @Override
    List<? extends CVParticle> execute(CVParticle input) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Generates the key for the provided tuple using the fields provided at construction time
     *
     * @param tuple
     * @return key created for this tuple or NULL if no key could be created (i.e. tuple does not contain any of groupBy Fields)
     */
    private String generateKey(Tuple tuple) {
        String key = new String();
        for (String field : groupBy) {
            key += tuple.getValueByField(field) + "_";
        }
        if (key.length() == 0) return null;
        return key;
    }

    /**
     * Callback method for removal of items from the histories cache. Items removed from the cache need to be acked or failed
     * according to the reason they were removed
     */
    @Override
    public void onRemoval(RemovalNotification<CVParticle, String> notification) {
        // make sure the CVParticle object is removed from the history (even if removal was automatic!)
        history.clear(notification.getKey(), notification.getValue());
        if (notification.getCause() == RemovalCause.EXPIRED || notification.getCause() == RemovalCause.SIZE) {
            // item removed automatically --> fail the tuple
            collector.fail(notification.getKey().getTuple());
        } else {
            // item removed explicitly --> ack the tuple
            collector.ack(notification.getKey().getTuple());
        }
    }

//    @Override
//    List<? extends CVParticle> execute(CVParticle input) throws Exception {
//        if (this.count == 0) {
//            this.startTime = System.currentTimeMillis();
//        }
//        // fill buffer with input.imageBytes first.
//        List<? extends CVParticle> results = operation.execute(input, new OperationHandler<Mat>() {
//            Logger logger1 = LoggerFactory.getLogger(OperationHandler.class);
//
//            @Override
//            public boolean fillSourceBufferQueue(Frame frame) {
//                // TODO Auto-generated method stub
//                byte[] encodedData = frame.getImageBytes();
//                // logger1.info("get image bytes length: "+encodedData.length);
//                while (!mSourceBufferQueue.fillBuffer(encodedData)) {
//                    logger1.info("atempt to fill buffer...");
//                }
//                return true;
//            }
//
//            @Override
//            public Mat getDecodedData() {
//                // TODO Auto-generated method stub
//                return mDecodedMatQueue.dequeue();
//            }
//
//            @Override
//            public byte[] getEncodedData(Mat proccessedResult) {
//                // TODO Auto-generated method stub
//                mResultMatQueue.enqueue(proccessedResult);
//
////				logger1.info("Before dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
//                Frame encodedFrame = null;
//                while ((encodedFrame = mEncodedFrameQueue.dequeue()) == null) {
//                    // logger1.info("encoded frame queue has no element!");
//                }
//                ;
////				 logger1.info("After dequeue "+mEncodedQueueId +" size: "+ mEncodedFrameQueue.getSize());
////				if((encodedFrame = mEncodedFrameQueue.dequeue())==null) {
////					return null;
////				} else {
////					return encodedFrame.getImageBytes();
////				}
//                return encodedFrame.getImageBytes();
//            }
//        });
//
//        this.count++;
//        if (this.count == 500) {
//            endTime = System.currentTimeMillis();
//            logger.info("{} Rate: {}", operation.getContext(), (500 / ((endTime - startTime) / 1000.0f)));
//            this.count = 0;
//        }
//
//        // copy metadata from input to output if configured to do so
////        for (CVParticle s : results) {
////            for (String key : input.getMetadata().keySet()) {
////                if (!s.getMetadata().containsKey(key)) {
////                    s.getMetadata().put(key, input.getMetadata().get(key));
////                }
////            }
////        }
//        return results;
//    }

//    // ----------------------------- HISTORY CONTAINER ---------------------------
//    /**
//     * Container that manages the history of tuples received and allows others to clean up the history retained.
//     *
//     * @author Corne Versloot
//     */
//    public class History implements Serializable {
//
//        private Cache<CVParticle, String> inputCache;
//        private HashMap<String, List<CVParticle>> groups;
//
//        /**
//         * Creates a History object for the specified Bolt (which is used to ack or fail items removed from the history).
//         *
//         * @param bolt
//         */
//        private History(BatchJPEGInputBolt bolt) {
//            groups = new HashMap<String, List<CVParticle>>();
//            inputCache = CacheBuilder.newBuilder()
//                    .maximumSize(maxSize)
//                    .expireAfterAccess(TTL, TimeUnit.SECONDS) // resets also on get(...)!
//                    .removalListener(bolt)
//                    .build();
//        }
//
//        /**
//         * Adds the new CVParticle object to the history and returns the list of items it was grouped with.
//         *
//         * @param group    the name of the group the CVParticle belongs to
//         * @param particle the CVParticle object that needs to be added to the history.
//         */
//        private void add(String group, CVParticle particle) {
//            if (!groups.containsKey(group)) {
//                groups.put(group, new ArrayList<CVParticle>());
//            }
//            List<CVParticle> list = groups.get(group);
//            int i;
//            for (i = list.size() - 1; i >= 0; i--) {
//                if (particle.getSequenceNr() > list.get(i).getSequenceNr()) {
//                    list.add(i + 1, particle);
//                    break;
//                }
//                if (refreshExperation) {
//                    inputCache.getIfPresent(list.get(i)); // touch the item passed in the cache to reset its expiration timer
//                }
//            }
//            if (i < 0) list.add(0, particle);
//            inputCache.put(particle, group);
//        }
//
//        /**
//         * Removes the object from the history. This will tricker an ACK to be send.
//         *
//         * @param particle
//         */
//        public void removeFromHistory(CVParticle particle) {
//            inputCache.invalidate(particle);
//        }
//
//        /**
//         * Removes the object from the group
//         *
//         * @param particle
//         * @param group
//         */
//        private void clear(CVParticle particle, String group) {
//            if (!groups.containsKey(group)) return;
//            groups.get(group).remove(particle);
//            if (groups.get(group).size() == 0) groups.remove(group);
//        }
//
//        /**
//         * Returns all the items in this history that belong to the specified group
//         *
//         * @param group
//         * @return
//         */
//        public List<CVParticle> getGroupedItems(String group) {
//            return groups.get(group);
//        }
//
//        public long size() {
//            return inputCache.size();
//        }
//
//        @Override
//        public String toString() {
//            String result = "";
//            for (String group : groups.keySet()) {
//                result += "  " + group + " : " + groups.get(group).size() + "\r\n";
//            }
//            return result;
//        }
//    }// end of History class


}
