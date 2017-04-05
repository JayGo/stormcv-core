package edu.fudan.stormcv.spout;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.fudan.storm.signals.spout.BaseSignalSpout;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.fetcher.IFetcher;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.util.LibLoader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Basic spout implementation that includes fault tolerance (if activated). It
 * should be noted that running the spout in fault tolerant mode will use more
 * memory because emitted tuples are cashed for a limited amount of time (which
 * is configurable).
 * <p>
 * THe actual reading of data is done by {@link IFetcher} implementations
 * creating {@link CVParticle} objects which are serialized and emitted by this
 * spout.
 *
 * @author Corne Versloot
 */
public class CVParticleSignalSpout extends BaseSignalSpout {

    private Logger logger = LoggerFactory.getLogger(CVParticleSignalSpout.class);
    private Cache<Object, Object> tupleCache; // a cache holding emitted tuples
    // so they can be replayed on
    // failure
    protected SpoutOutputCollector collector;
    private boolean faultTolerant = false;
    private IFetcher<? extends CVParticle> fetcher;

    public CVParticleSignalSpout(String zkName, IFetcher<? extends CVParticle> fetcher) {
        super(zkName);
        this.fetcher = fetcher;
    }

    /**
     * Indicates if this Spout must cache tuples it has emitted so they can be
     * replayed on failure. This setting does not effect anchoring of tuples
     * (which is always done to support TOPOLOGY_MAX_SPOUT_PENDING
     * configuration)
     *
     * @param faultTolerant
     * @return
     */
    public CVParticleSignalSpout setFaultTolerant(boolean faultTolerant) {
        this.faultTolerant = faultTolerant;
        return this;
    }

    /**
     * Configures the spout by fetching optional parameters from the provided
     * configuration. If faultTolerant is true the open function will also
     * construct the cache to hold the emitted tuples. Configuration options
     * are:
     * <ul>
     * <li>stormcv.faulttolerant --> boolean: indicates if the spout must
     * operate in fault tolerant mode (i.e. replay tuples after failure)</li>
     * <li>stormcv.tuplecache.timeout --> long: timeout (seconds) for tuples in
     * the cache</li>
     * <li>stormcv.tuplecache.maxsize --> int: maximum number of tuples in the
     * cache (used to avoid memory overload)</li>
     * </ul>
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        if (conf.containsKey(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT)) {
            faultTolerant = (Boolean) conf
                    .get(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT);
        }
        if (faultTolerant) {
            long timeout = conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC) == null ? 30
                    : (Long) conf.get(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC);
            int maxSize = conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE) == null ? 500
                    : ((Long) conf.get(StormCVConfig.STORMCV_CACHES_MAX_SIZE))
                    .intValue();
            tupleCache = CacheBuilder.newBuilder().maximumSize(maxSize)
                    .expireAfterAccess(timeout, TimeUnit.SECONDS).build();
        }

        // pass configuration to subclasses
        try {
            fetcher.prepare(conf, context);
        } catch (Exception e) {
            logger.warn("Unable to configure spout due to ", e);
        }

        LibLoader.loadOpenCVLib();

       logger.info("[jkyan]zkHosts:{}", zkHosts(conf));
    }

    private static String zkHosts(Map conf) {
        int zkPort = Utils.getInt(conf.get("storm.zookeeper.port")).intValue();
        List zkServers = (List)conf.get("storm.zookeeper.servers");
        Iterator it = zkServers.iterator();
        StringBuffer sb = new StringBuffer();

        while(it.hasNext()) {
            sb.append((String)it.next());
            sb.append(":");
            sb.append(zkPort);
            if(it.hasNext()) {
                sb.append(",");
            }
        }
        System.out.println(sb.toString());
        return sb.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fetcher.getSerializer().getFields());
    }

    @Override
    public void nextTuple() {
        CVParticle particle = fetcher.fetchData();

        if (particle != null) {
            try {
                Values values = fetcher.getSerializer().toTuple(particle);
                String id = particle.getStreamId() + "_"
                        + particle.getSequenceNr();
                if (faultTolerant && tupleCache != null)
                    tupleCache.put(id, values);
                collector.emit(values, id);
            } catch (IOException e) {
                logger.warn("Unable to fetch next frame from queue due to: "
                        + e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        if (faultTolerant && tupleCache != null) {
            tupleCache.cleanUp();
        }
        fetcher.deactivate();
    }

    @Override
    public void activate() {
        fetcher.activate();
    }

    @Override
    public void deactivate() {
        fetcher.deactivate();
    }

    @Override
    public void ack(Object msgId) {
        if (faultTolerant && tupleCache != null) {
            tupleCache.invalidate(msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        logger.debug("Fail of: " + msgId);
        if (faultTolerant && tupleCache != null
                && tupleCache.getIfPresent(msgId) != null) {
            collector.emit((Values) tupleCache.getIfPresent(msgId), msgId);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void onSignal(byte[] bytes) {
        fetcher.onSignal(bytes);
    }
}
