package edu.fudan.stormcv.model;

import edu.fudan.stormcv.constant.BoltOperationType;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/2/17 - 3:58 AM
 * Description:
 */
public class ImageHandle extends CVParticle {

    private List<String> locations;

    private BoltOperationType type;

    public ImageHandle(String streamId, long sequenceNr, List<String> locations, BoltOperationType type) {
        super(streamId, sequenceNr);
        this.locations = locations;
        this.type = type;
    }

    public ImageHandle(Tuple tuple, List<String> locations, BoltOperationType type) {
        super(tuple);
        this.locations = locations;
        this.type = type;
    }

    public List<String> getLocations() {
        return this.locations;
    }

    public int getLocationSize() {
        return this.locations.size();
    }

    public BoltOperationType getType() {
        return this.type;
    }

}
