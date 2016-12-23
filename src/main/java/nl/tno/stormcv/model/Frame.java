package nl.tno.stormcv.model;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.Tuple;
import org.opencv.core.CvType;

import nl.tno.stormcv.util.ImageUtils;

/**
 * A {@link CVParticle} object representing a frame from a video stream (or part of a frame called a Tile). Additional to the fields inherited from
 * {@link CVParticle} the Frame object has the following fields:
 * <ul>
 * <li>timeStamp: relative timestamp in ms of this frame with respect to the start of the stream (which is 0 ms)</li>
 * <li>image: the actual frame represented as BufferedImage <i>(may be NULL)</i></li>
 * <li>boundingBox: indicates which part of the original frame this object represents. This might be the full frame but can also be 
 * some other region of interest. This box can be used for tiling and combining purposes as well.</li>
 * <li>features: list with {@link Feature} objects (containing one or more {@link Descriptor}) describing 
 * aspects of the frame such as SIFT, color histogram or Optical FLow (list may be empty)</li>
 * </ul>
 * 
 * A Frame can simply function as a container for multiple Features and hence may not actually have an image. In this case getImageType will 
 * return Frame.NO_IMAGE and getImage will return null.
 * 
 * @author Corne Versloot
 *
 */
public class Frame extends CVParticle {

	public final static String NO_IMAGE = "none";
	public final static String JPG_IMAGE = "jpg";
	public final static String PNG_IMAGE = "png";
	public final static String GIF_IMAGE = "gif";
	public final static String RAW_IMAGE = "mat";  //add by jkyan at 2016/07/30
	public final static String X264_Bytes = "x264_bytes"; // add by jiu on 2016/12/12
	
	private long timeStamp;
	private String imageType = JPG_IMAGE;
	private byte[] imageBytes = null;
	private BufferedImage image;
	private Rectangle boundingBox;
	private List<Feature> features = new ArrayList<Feature>();
	private boolean needGen = false;
	
	public Frame(byte [] imageBytes) {
		this.imageBytes = imageBytes;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, BufferedImage image, long timeStamp, Rectangle boundingBox, List<Feature> features) throws IOException {
		this(streamId, sequenceNr, imageType, image, timeStamp, boundingBox);
		if(features != null) this.features = features;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, BufferedImage image, long timeStamp, Rectangle boundingBox ) throws IOException {
		super( streamId, sequenceNr);
		this.imageType = imageType;
		setImage(image);
		this.timeStamp = timeStamp;
		this.boundingBox = boundingBox;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, byte[] imageBytes, long timeStamp, Rectangle boundingBox, List<Feature> features) {
		this(streamId, sequenceNr, imageType, imageBytes, timeStamp, boundingBox);
		if(features != null) this.features = features;
	}
	
	public Frame(String streamId, long sequenceNr, String imageType, byte[] imageBytes, long timeStamp, Rectangle boundingBox ) {
		super(streamId, sequenceNr);
		this.imageType = imageType;
		this.imageBytes = imageBytes;
		this.timeStamp = timeStamp;
		this.boundingBox = boundingBox;
	}
		
	public Frame(Tuple tuple, String imageType, byte[] image, long timeStamp, Rectangle box) {
		super(tuple);
		this.imageType = imageType;
		this.imageBytes = image;
		this.timeStamp = timeStamp;
		this.boundingBox = box;
	}
	
	public Rectangle getBoundingBox() {
		return boundingBox;
	}
	
//	public BufferedImage getImage() throws IOException {
//		if(imageBytes == null) {
//			imageType = NO_IMAGE;
//			return null;
//		}
//		if(image == null){
//			image = ImageUtils.bytesToImage(imageBytes);
//		}
//		return image;
//	}
	
	public BufferedImage getImage(){
		if (image != null) {
			return image;
		}
		if(imageBytes == null) {
			imageType = NO_IMAGE;
			return null;
		}
		if(imageType.equals(RAW_IMAGE)){
			image = ImageUtils.matBytesToBufferedImage(imageBytes, boundingBox.width, boundingBox.height, CvType.CV_8UC3);
			//image = ImageUtils.bytesToBufferedImage(imageBytes, boundingBox.width, boundingBox.width, BufferedImage.TYPE_3BYTE_BGR);
		} else {
			try {
				image = ImageUtils.bytesToImage(imageBytes);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		needGen = false;
		return image;
	}

//	public void setImage(BufferedImage image) throws IOException {
//		this.image = image;
//		if(image != null){
//			if(imageType.equals(NO_IMAGE)) imageType = JPG_IMAGE;
//			this.imageBytes = ImageUtils.imageToBytes(image, imageType);
//		}else{
//			this.imageBytes = null;
//			this.imageType = NO_IMAGE;
//		}
//	}
	
	public void setImage(BufferedImage image) throws IOException {
		this.image = image;
		this.needGen = true;
	}
		
	private void updataImageBytes() throws IOException {
		if (needGen) {
			if (image != null) {
				if(imageType.equals(NO_IMAGE)) {
					imageType = JPG_IMAGE;
				}
				if(imageType.equals(RAW_IMAGE)) {
					this.imageBytes = ImageUtils.imageToBytesWithOpenCV(image);
				} else {
					this.imageBytes = ImageUtils.imageToBytes(image, imageType);
				}
			} else {
				this.imageBytes = null;
				this.imageType = NO_IMAGE;
			}
		}
	}
	
	public void setImage(byte[] imageBytes, String imgType){
		this.imageBytes = imageBytes;
		this.imageType = imgType;
		this.image = null;
	}
	
	public void swapImageBytes(byte[] imageBytes) {
		this.imageBytes = imageBytes;
	}
	
	public void removeImage(){
		this.image = null;
		this.imageBytes = null;
		this.imageType = NO_IMAGE;
	}


	public long getTimestamp(){
		return this.timeStamp;
	}

	public List<Feature> getFeatures() {
		return features;
	}

	public String getImageType() {
		return imageType;
	}

//	public void setImageType(String imageType) throws IOException {
//		this.imageType = imageType;
//		if(image != null){
//			imageBytes = ImageUtils.imageToBytes(image, imageType);
//		}else{
//			image = ImageUtils.bytesToImage(imageBytes);
//			imageBytes = ImageUtils.imageToBytes(image, imageType);
//		}
//	}
	
//	public void setImageType(String imageType) throws IOException {
//		this.imageType = imageType;
//		if(image != null){
//			if (imageType.equals(RAW_IMAGE)) {
//				imageBytes = ImageUtils.imageToBytesWithOpenCV(image);
//			} else {
//				imageBytes = ImageUtils.imageToBytes(image, imageType);
//			}
//		} else {
//			if (imageBytes != null) {
//				if (imageType.equals(RAW_IMAGE))
//				image = ImageUtils.bytesToImage(imageBytes);
//				imageBytes = ImageUtils.imageToBytes(image, imageType);
//			}
//			
//		}
//	}

//	public byte[] getImageBytes() {
//		return imageBytes;
//	}
	
	public byte[] getImageBytes() {
		return imageBytes;
	}
	
//	public byte[] getImageBytes() throws IOException {
//		if (imageBytes == null) {
//			if (image != null) {
//				if (imageType.equals(RAW_IMAGE)) {
//					imageBytes = ImageUtils.imageToBytesWithOpenCV(image);
//				} else {
//					imageBytes = ImageUtils.imageToBytes(image, imageType);
//				}
//			} else {
//				return null;
//			}
//		}
//		return imageBytes;
//	}

	@Override
	public String toString() {
		String result= "Frame : {streamId:"+getStreamId()+", sequenceNr:"+getSequenceNr()+", timestamp:"+getTimestamp()+", imageType:"+imageType+", bytesLength:"+imageBytes.length+", features:[ ";
		for(Feature f : features) result += f.getName()+" = "+f.getSparseDescriptors().size()+", ";
		return result + "] }";
	}
}

