package edu.fudan.stormcv.bolt;

import clojure.lang.PersistentArrayMap;
import edu.fudan.storm.signals.bolt.BaseSignalBolt;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StormCV's basic BaseRichBolt implementation that supports the use of
 * {@link CVParticle} objects. This bolt supports fault tolerance if it is
 * configured to do so and supports the serialization of model objects.
 *
 * @author Corne Versloot
 */
public abstract class CVParticleSignalBolt extends BaseSignalBolt {

    protected Logger logger = LoggerFactory.getLogger(CVParticleSignalBolt.class);
    protected HashMap<String, CVParticleSerializer<? extends CVParticle>> serializers = new HashMap<String, CVParticleSerializer<? extends CVParticle>>();
    protected OutputCollector collector;
    protected String boltName;
    protected long idleTimestamp = -1;

    public CVParticleSignalBolt(String name) {
        super(name);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.boltName = context.getThisComponentId();

        try {
            PersistentArrayMap map = (PersistentArrayMap) conf
                    .get(Config.TOPOLOGY_KRYO_REGISTER);
            for (Object className : map.keySet()) {
                serializers.put((String) className,
                        (CVParticleSerializer<? extends CVParticle>) Class
                                .forName((String) map.get(className))
                                .newInstance());
            }
        } catch (Exception e) {
            logger.error("Unable to prepare CVParticleBolt due to ", e);
        }

        this.prepare(conf, context);
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void execute(Tuple input) {
        try {
            CVParticle cvt = deserialize(input);
            List<? extends CVParticle> results = execute(cvt);
            for (CVParticle output : results) {
                output.setRequestId(cvt.getRequestId());
                CVParticleSerializer serializer = serializers.get(output
                        .getClass().getName());
                if (serializers.containsKey(output.getClass().getName())) {
                    Values values = serializer.toTuple(output);
                    collector.emit(input, values);
//					collector.emit(values);
                } else {
                    // TODO: what else?
                }
            }
            collector.ack(input);
        } catch (Exception e) {
            logger.warn("Unable to process input", e);
            collector.fail(input);
        }
        idleTimestamp = System.currentTimeMillis();
    }

    /**
     * Deserializes a Tuple into a CVParticle type
     *
     * @param tuple
     * @return
     * @throws IOException
     */
    protected CVParticle deserialize(Tuple tuple) throws IOException {
        String typeName = tuple.getStringByField(CVParticleSerializer.TYPE);
        return serializers.get(typeName).fromTuple(tuple);
    }

    /**
     * Subclasses must implement this method which is responsible for analysis
     * of received CVParticle objects. A single input object may result in zero
     * or more resulting objects which will be serialized and emitted by this
     * Bolt.
     *
     * @param input
     * @return
     */
    abstract List<? extends CVParticle> execute(CVParticle input)
            throws Exception;

    @SuppressWarnings("rawtypes")
    abstract void prepare(Map stormConf, TopologyContext context);
}
