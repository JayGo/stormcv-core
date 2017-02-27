package nl.tno.stormcv.model.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.MatImage;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MatImageSerializer extends Serializer<MatImage> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(MatImageSerializer.class);
    public static final String STREAMID = "streamID";
    public static final String SEQUENCENR = "sequenceNR";
    public static final String TYPE = "type";
    public static final String WIDTH = "width";
    public static final String HEIGHT = "height";
    public static final String TIMESTAMP = "timestamp";
    public static final String IMAGETYPE = "imagetype";
    public static final String MATBYTES = "matbytes";

    /**
     * Generates a Type Object from the provided tuple
     *
     * @param tuple
     * @return
     * @throws IOException
     */
    public MatImage fromTuple(Tuple tuple) throws IOException {
        MatImage type = createObject(tuple);
        return type;
    }

    /**
     * Converts a Type Object to a Values tuple which can be emitted by storm
     *
     * @param
     * @return
     * @throws IOException
     */
    public Values toTuple(MatImage matImage) throws IOException {
        Values values = new Values(matImage.getClass().getName(),
                matImage.getStreamId(), matImage.getSequence(),
                matImage.getWidth(), matImage.getHeight(),
                matImage.getTimestamp(), matImage.getMatbytes(), matImage.getType());
        return values;
    }

    public Values toFrameTuple(MatImage matImage) throws IOException {
        FrameSerializer serializer = new FrameSerializer();
        Values values = null;
        Frame frame = matImage.convertToFrame();
        if (frame != null) {
            values = serializer.toTuple(frame);
        }
        return values;
    }

    /**
     * Returns the field names of the tuple generated by this serializer (also
     * see toTuple)
     *
     * @return
     */
    public Fields getFields() {
        List<String> fields = new ArrayList<String>();
        fields.add(TYPE);
        fields.add(STREAMID);
        fields.add(SEQUENCENR);
        fields.add(WIDTH);
        fields.add(HEIGHT);
        fields.add(TIMESTAMP);
        fields.add(MATBYTES);
        fields.add(IMAGETYPE);
        return new Fields(fields);
    }

    @Override
    public MatImage read(Kryo kryo, Input input, Class<MatImage> clas) {
        String streamId = input.readString();
        long sequenceNr = input.readLong();
        int width = input.readInt();
        int height = input.readInt();
        long timestamp = input.readLong();
        int buffSize = input.readInt();
        int type = input.readInt();
        byte[] buffer = null;
        if (buffSize > 0) {
            buffer = new byte[buffSize];
            input.readBytes(buffer);
        }
        try {
            MatImage result = new MatImage(buffer, streamId, width, height,
                    timestamp, sequenceNr, type);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(Kryo kryo, Output output, MatImage type) {
        output.writeString(type.getStreamId());
        output.writeLong(type.getSequence());
        output.writeInt(type.getWidth());
        output.writeInt(type.getHeight());
        output.writeLong(type.getTimestamp());
        //byte[] buffer = ImageUtils.matToBytes(type.getMat());
        byte[] buffer = type.getMatbytes();
        if (buffer != null) {
            output.writeInt(buffer.length);
            logger.debug("write __seq:" + type.getSequence() + "   length:" + buffer.length);
            output.writeBytes(buffer);
        } else {
            logger.debug("mat bytes is null");
            output.writeInt(0);
        }
        output.flush();
        // output.close();
    }

    protected MatImage createObject(Tuple tuple) throws IOException {
        byte[] buffer = tuple.getBinaryByField(MATBYTES);
        MatImage matImage = null;
//		System.out.println("create__seq " + tuple.getLongByField(SEQUENCENR)
//				+ "buffer:" + buffer.length + " matsize:"
//				+ tuple.getIntegerByField(WIDTH) + "x"
//				+ tuple.getIntegerByField(HEIGHT));
        if (buffer != null) {
            matImage = new MatImage(buffer, tuple.getStringByField(STREAMID),
                    tuple.getIntegerByField(WIDTH),
                    tuple.getIntegerByField(HEIGHT),
                    tuple.getLongByField(TIMESTAMP),
                    tuple.getLongByField(SEQUENCENR),
                    tuple.getIntegerByField(IMAGETYPE));
        }
        return matImage;
    }

    public byte[] toBytes(MatImage matImage) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(MatImage.class);
        Output output = new Output(1024, 1024000);
        output.writeString(matImage.getStreamId());
        output.writeLong(matImage.getSequence());
        output.writeInt(matImage.getWidth());
        output.writeInt(matImage.getHeight());
        output.writeLong(matImage.getTimestamp());
        byte[] buffer = matImage.getMatbytes();
        //byte[] buffer = ImageUtils.matToBytes(matImage.getMat());
        if (buffer != null) {
            output.writeInt(buffer.length);
            output.writeBytes(buffer);
        } else {
            output.writeInt(0);
        }
        byte[] bytes = output.toBytes();
        output.flush();
        output.clear();
        output.close();
        return bytes;
    }

    public MatImage fromBytes(byte[] bytes) {
        Input input = new Input(bytes);
        String streamId = input.readString();
        long sequenceNr = input.readLong();
        int width = input.readInt();
        int height = input.readInt();
        long timestamp = input.readLong();
        int buffSize = input.readInt();
        int type = input.readInt();
        byte[] buffer = null;
        if (buffSize > 0) {
            buffer = new byte[buffSize];
            input.readBytes(buffer);
        }
        MatImage result = null;
        try {
            result = new MatImage(buffer, streamId, width, height,
                    timestamp, sequenceNr, type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        input.close();
        return result;
    }
}
