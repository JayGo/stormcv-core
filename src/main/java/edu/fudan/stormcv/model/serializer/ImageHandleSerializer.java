package edu.fudan.stormcv.model.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.fudan.stormcv.constant.BOLT_OPERTION_TYPE;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.ImageHandle;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/2/17 - 6:29 AM
 * Description:
 */
public class ImageHandleSerializer extends CVParticleSerializer<ImageHandle> implements Serializable {

//    public static final String PREFIX = "prefix";
    public static final String LOCATION_LIST = "location_list";
    public static final String OPERATION_TYPE = "operation_type";

    @Override
    protected void writeObject(Kryo kryo, Output output, ImageHandle imageHandle) throws Exception {
    }

    @Override
    protected ImageHandle readObject(Kryo kryo, Input input, Class<ImageHandle> clas, long requestId, String streamId, long sequenceNr) throws Exception {
        return null;
    }

    @Override
    protected ImageHandle createObject(Tuple tuple) throws IOException {
        return new ImageHandle(tuple, (List<String>)tuple.getValueByField(LOCATION_LIST),
                (BOLT_OPERTION_TYPE)tuple.getValueByField(OPERATION_TYPE));
    }

    @Override
    protected Values getValues(CVParticle object) throws IOException {
        Values values = new Values();
        ImageHandle imageHandle = (ImageHandle)object;
        values.add(imageHandle.getLocations());
        values.add(imageHandle.getType());
        return values;
    }

    @Override
    protected List<String> getTypeFields() {
        List<String> fields = new ArrayList<>();
        fields.add(LOCATION_LIST);
        fields.add(OPERATION_TYPE);
        return fields;
    }
}
