package nl.tno.stormcv.model.serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.VideoChunk;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class VideoChunkSerializer extends CVParticleSerializer<VideoChunk> implements Serializable{

	private static final long serialVersionUID = 7864791957493203664L;

	public static final String VIDEO = "video";
	public static final String TIMESTAMP = "timeStamp";
	public static final String CONTAINER = "container";
	
	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(TIMESTAMP);
		fields.add(VIDEO);
		fields.add(CONTAINER);
		return fields;
	}
	
	@Override
	protected Values getValues(CVParticle object) throws IOException {
		VideoChunk video = (VideoChunk)object;
		return new Values(video.getDuration(), video.getVideo(), video.getContainer());
	}
	
	@Override
	protected VideoChunk createObject(Tuple tuple) throws IOException {
		return new VideoChunk(tuple, tuple.getLongByField(TIMESTAMP), 
				tuple.getBinaryByField(VIDEO), tuple.getStringByField(CONTAINER));
	}
	
	@Override
	protected void writeObject(Kryo kryo, Output output, VideoChunk video) throws Exception {
		output.writeLong(video.getDuration());
		output.writeInt(video.getVideo().length);
		output.writeBytes(video.getVideo());
		output.writeString(video.getContainer());
	}

	@Override
	protected VideoChunk readObject(Kryo kryo, Input input, Class<VideoChunk> clas, long requestId, String streamId, long sequenceNr) throws Exception {
		long duration = input.readLong();
		int length = input.readInt();
		byte[] video = input.readBytes(length);
		String container = input.readString();
		VideoChunk chunk = new VideoChunk(streamId, sequenceNr, duration, video, container);
		chunk.setRequestId(requestId);
		return chunk;
	}



}
