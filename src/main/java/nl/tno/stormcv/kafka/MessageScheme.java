package nl.tno.stormcv.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;


/**
 * this class used to set the message type translantion of kafka-spout
 * @params byte[] bytes
 * @return Values tuple
 * @author jkyan  
 **/
public class MessageScheme implements Scheme {

	private static final long serialVersionUID = -6756499411066405353L;
	private static Logger logger = LoggerFactory.getLogger(MessageScheme.class);
	FrameSerializer serializer = new FrameSerializer();
	
    /* (non-Javadoc)
     * @see backtype.storm.spout.Scheme#deserialize(byte[])
     */
    /*public List<Object> deserialize(byte[] bytes) {
    	Frame frame = serializer.fromBytes(bytes);
    	//check
    	if (frame != null) {
    		//logger.info("spout receive a frame with streamId " + frame.getStreamId() + " seq " + frame.getSequenceNr());
    	} else {
    		logger.error("spout can not get a not null frame!");
    	}
    	try {
			if (frame.getImage() == null) {
				logger.error("spout: image is null!");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

    	try {
			return serializer.toTuple(frame);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return null;
    }*/

	@Override
	public List<Object> deserialize(ByteBuffer byteBuffer) {
		byte[] bytes = new byte[byteBuffer.remaining()];
		byteBuffer.get(bytes, 0, bytes.length);
		Frame frame = serializer.fromBytes(bytes);
		//check
		if (frame != null && frame.getImageBytes() != null) {
			try {
				logger.info("read a frame " + frame.getBoundingBox().getWidth() + "x" + frame.getBoundingBox().getHeight());
				return serializer.toTuple(frame);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			logger.error("spout can not get a not null frame!");
		}

		return null;
	}

	/* (non-Javadoc)
         * @see backtype.storm.spout.Scheme#getOutputFields()
         */
    @Override
	public Fields getOutputFields() {
    	return serializer.getFields();
    }  
} 