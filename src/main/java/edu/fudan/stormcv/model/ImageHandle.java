package edu.fudan.stormcv.model;

import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/2/17 - 3:58 AM
 * Description:
 */
public class ImageHandle extends CVParticle {

    private List<String> locations;

    private BOLT_OPERTION_TYPE type;

    public ImageHandle(String streamId, long sequenceNr, List<String> locations, BOLT_OPERTION_TYPE type) {
        super(streamId, sequenceNr);
        this.locations = locations;
        this.type = type;
    }

    public ImageHandle(Tuple tuple, List<String> locations, BOLT_OPERTION_TYPE type) {
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

    public BOLT_OPERTION_TYPE getType() {
        return this.type;
    }

}
