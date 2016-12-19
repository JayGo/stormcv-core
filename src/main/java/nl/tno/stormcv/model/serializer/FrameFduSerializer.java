package nl.tno.stormcv.model.serializer;

import java.awt.Rectangle;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.FrameFdu;

public class FrameFduSerializer extends CVParticleSerializer<FrameFdu> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7816933982964656713L;
	public static final String IMAGETYPE = "imagetype";
	public static final String IMAGE = "imagebytes";
	public static final String TIMESTAMP = "timeStamp";
	public static final String BOUNDINGBOX = "boundingbox";
	public static final String FEATURES = "features";
	
	@Override
	protected Values getValues(CVParticle object) throws IOException {
		// TODO Auto-generated method stub
		FrameFdu frameFdu = (FrameFdu) object;
		
		// The order must be same as the add-order in getTypeFileds()
		return new Values(frameFdu.getImageType(), frameFdu.getImageBytes(), 
				frameFdu.getTimeStamp(), frameFdu.getBoundingBox(), frameFdu.getFeatures());
	}
	
	@Override
	protected List<String> getTypeFields() {
		// TODO Auto-generated method stub
		List<String> fields = new ArrayList<String>();
		fields.add(IMAGETYPE);
		fields.add(IMAGE);
		fields.add(TIMESTAMP);
		fields.add(BOUNDINGBOX);
		fields.add(FEATURES);
		return fields;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected FrameFdu createObject(Tuple tuple) throws IOException {
		// TODO Auto-generated method stub
		FrameFdu frameFdu = new FrameFdu(tuple);
		String imageType = tuple.getStringByField(IMAGETYPE);
		byte[] buffer = tuple.getBinaryByField(IMAGE);
		long timeStamp = tuple.getLongByField(TIMESTAMP);
		Rectangle box = (Rectangle) tuple.getValueByField(BOUNDINGBOX);
		
		List<Feature> features = (List<Feature>) tuple.getValueByField(FEATURES);
		
		frameFdu.setImageType(imageType);
		frameFdu.setImageBytes(buffer);
		frameFdu.setTimeStamp(timeStamp);
		frameFdu.setBoundingBox(box);
		frameFdu.getFeatures().addAll(features);

		return frameFdu;
	}
	
	@Override
	protected void writeObject(Kryo kryo, Output output, FrameFdu frame) throws Exception {
		// TODO Auto-generated method stub
		output.writeLong(frame.getTimeStamp());
		output.writeString(frame.getImageType());
		byte[] buffer = frame.getImageBytes();
		if (buffer != null) {
			output.writeInt(buffer.length);
			output.writeBytes(buffer);
		} else {
			output.writeInt(0);
		}
		output.writeFloat((float) frame.getBoundingBox().getX());
		output.writeFloat((float) frame.getBoundingBox().getY());
		output.writeFloat((float) frame.getBoundingBox().getWidth());
		output.writeFloat((float) frame.getBoundingBox().getHeight());

		kryo.writeObject(output, frame.getFeatures());
	}

	// the oder of reading must be same as the order of writing
	@SuppressWarnings("unchecked")
	@Override
	protected FrameFdu readObject(Kryo kryo, Input input, Class<FrameFdu> clas, long requestId, String streamId,
			long sequenceNr) throws Exception {
		// TODO Auto-generated method stub
		long timeStamp = input.readLong();
		String imageType = input.readString();
		int buffSize = input.readInt();
		byte[] buffer = null;
		if (buffSize > 0) {
			buffer = new byte[buffSize];
			input.readBytes(buffer);
		}
		Rectangle boundingBox = new Rectangle(Math.round(input.readFloat()),
				Math.round(input.readFloat()), Math.round(input.readFloat()),
				Math.round(input.readFloat()));
		List<Feature> features = kryo.readObject(input, ArrayList.class);

		FrameFdu frame = new FrameFdu(streamId, sequenceNr, imageType, buffer,
				timeStamp, boundingBox, features);
		frame.setRequestId(requestId);
		return frame;
	}







}
