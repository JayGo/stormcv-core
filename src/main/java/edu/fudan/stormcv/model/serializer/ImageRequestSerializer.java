package edu.fudan.stormcv.model.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.fudan.stormcv.model.ImageRequest;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/3/17 - 6:07 AM
 * Description:
 */
public class ImageRequestSerializer extends Serializer<ImageRequest> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, ImageRequest imageRequest) {
        output.writeString(imageRequest.getStreamId());
        output.writeString(imageRequest.getInputLocation());
        output.writeString(imageRequest.getOutputLocation());
        output.writeInt(imageRequest.getProcessType());
        output.flush();
    }

    @Override
    public ImageRequest read(Kryo kryo, Input input, Class<ImageRequest> aClass) {
        String streamId = input.readString();
        String inputLocation = input.readString();
        String outputLocation = input.readString();
        int type = input.readInt();
        ImageRequest request = new ImageRequest(streamId, inputLocation, outputLocation, type);
        return request;
    }
}
