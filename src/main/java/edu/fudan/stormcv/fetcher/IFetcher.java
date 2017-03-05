package edu.fudan.stormcv.fetcher;

import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.spout.CVParticleSpout;
import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Fetcher implementations are executed within {@link CVParticleSpout} and are responsible to read data and push it into topologies.
 * Fetcher's produce a {@link CVParticle} implementation which has its own {@link CVParticleSerializer} implementation as well.
 *
 * @author Corne Versloot
 */

public interface IFetcher<Output extends CVParticle> extends Serializable {

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) throws Exception;

    public CVParticleSerializer<Output> getSerializer();

    public void activate();

    public void deactivate();

    public Output fetchData();

    public void onSignal(byte[] bytes);
}
