package nl.tno.stormcv.model;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.Tuple;

public class FrameFdu extends CVParticle{
	
	public final static String H_264 = "h264";
	
	private long timeStamp;
	private String imageType = H_264;
	private byte[] imageBytes = null;
	private Rectangle boundingBox = null;
	private List<Feature> features = new ArrayList<Feature>();
	
	public FrameFdu(Tuple tuple) {
		super(tuple);
		// TODO Auto-generated constructor stub
	}
	
	public FrameFdu(Tuple tuple, long timeStamp, String imageType, byte[] imageBytes) {
		super(tuple);
		this.timeStamp = timeStamp;
		this.imageType = imageType;
		this.imageBytes = imageBytes;
	}

	
	public FrameFdu(String streamId, long sequenceNr, String imageType, byte[] buffer, long timeStamp, Rectangle boundingBox, List<Feature> features) {
		// TODO Auto-generated constructor stub
		super(streamId, sequenceNr);
		this.timeStamp = timeStamp;
		this.imageType = imageType;
		this.imageBytes = buffer;
		this.boundingBox = boundingBox;
		if(features != null) this.features = features;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getImageType() {
		return imageType;
	}

	public void setImageType(String imageType) {
		this.imageType = imageType;
	}

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public void setImageBytes(byte[] imageBytes) {
		this.imageBytes = imageBytes;
	}

	public Rectangle getBoundingBox() {
		return boundingBox;
	}

	public void setBoundingBox(Rectangle boundingBox) {
		this.boundingBox = boundingBox;
	}

	public List<Feature> getFeatures() {
		return features;
	}

	public void setFeatures(List<Feature> features) {
		this.features = features;
	}
	
	@Override
	public String toString() {
		String result= "Frame : {streamId:"+getStreamId()+", sequenceNr:"+getSequenceNr()+", timestamp:"+getTimeStamp()+", imageType:"+imageType+", features:[ ";
		for(Feature f : features) result += f.getName()+" = "+f.getSparseDescriptors().size()+", ";
		return result + "] }";
	}


}
