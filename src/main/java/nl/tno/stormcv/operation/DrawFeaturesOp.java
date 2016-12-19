package nl.tno.stormcv.operation;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.fudan.lwang.codec.OperationHandler;
import nl.tno.stormcv.model.*;
import nl.tno.stormcv.model.serializer.*;
import nl.tno.stormcv.util.ImageUtils;
import nl.tno.stormcv.util.connector.ConnectorHolder;
import nl.tno.stormcv.util.connector.FileConnector;
import nl.tno.stormcv.util.connector.LocalFileConnector;

/**
 * Draws the features contained within a {@link Frame} on the image itself and is primarily used for testing purposes.
 * The bounding boxes are drawn to indicate the location of a {@link Descriptor}. If a <code>writeLocation</code> is provided the image will be written
 * to that location as PNG image with filename: streamID_sequenceNR_randomNR.png. 
 *  
 * @author Corne Versloot
 */
public class DrawFeaturesOp implements ISingleInputOperation<Frame> {

	private static final long serialVersionUID = 5628467120758880353L;
	private Logger logger = LoggerFactory.getLogger(DrawFeaturesOp.class);
	private FrameSerializer serializer = new FrameSerializer();
	private String writeLocation;
	private ConnectorHolder connectorHolder;
	private boolean drawMetadata = false;
	//private Logger logger = LoggerFactory.getLogger(getClass());
	private static int count = 0;
	private static int count1 = 0;
	
	public DrawFeaturesOp(){	}
	
	public DrawFeaturesOp destination(String location){
		this.writeLocation = location;
		return this;
	}
	
	public String name() {
		return "drawer";
	}
	
	/**
	 * Indicates if metadata attached to the Frame must be written in the frame as well 
	 * (as 'key = value' in the upper left corner)
	 * @param bool
	 * @return itself
	 */
	public DrawFeaturesOp drawMetadata(boolean bool){
		this.drawMetadata = bool;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) throws Exception {
		this.connectorHolder = new ConnectorHolder(stormConf);
	}

	@Override
	public void deactivate() {	}

	@Override
	public CVParticleSerializer<Frame> getSerializer() {
		return serializer;
	}

	@Override
	public List<Frame> execute(CVParticle particle) throws Exception {
		List<Frame> result = new ArrayList<Frame>();
		if(!(particle instanceof Frame)) return result;
		Frame sf = (Frame)particle;
		result.add(sf);
		BufferedImage image = sf.getImage();
		
		if(image == null) return result;
		
		Graphics2D graphics = image.createGraphics();
		int colorIndex = 0;
		for(Feature feature : sf.getFeatures()){
			graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
			for(Descriptor descr : feature.getSparseDescriptors()){
				Rectangle box = descr.getBoundingBox().getBounds();
				if(box.width == 0 ) box.width = 1;
				if(box.height == 0) box.height = 1;
				graphics.draw(box);
			}
			colorIndex++;
		}
		
		int y = 10;
		// draw feature legenda on top of everything else
		for(colorIndex = 0; colorIndex < sf.getFeatures().size(); colorIndex++){
			graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
			graphics.drawString(sf.getFeatures().get(colorIndex).getName(), 5, y);
			y += 12;
		}
		
		if(drawMetadata) for(String key : sf.getMetadata().keySet()){
			colorIndex++;
			graphics.setColor(ImageUtils.colors[colorIndex % ImageUtils.colors.length]);
			graphics.drawString(key+" = "+sf.getMetadata().get(key), 5, y);
			y += 12;
		}
			
		sf.setImage(image);
		if(writeLocation != null) {
			//String destination = writeLocation + (writeLocation.endsWith("/") ? "" 
			//	: "/") + sf.getStreamId()+"_"+sf.getSequenceNr()+"_"+Math.random()+".png";
			Long timeStamp = System.currentTimeMillis();
			String destination = writeLocation + (writeLocation.endsWith("/") ? "" : "/") 
					+ sf.getStreamId() + "_" + timeStamp + "." + sf.getImageType();
			System.out.println("des:" + destination );
			FileConnector fl = connectorHolder.getConnector(destination);
			if(fl != null){
				fl.moveTo(destination);
				if(fl instanceof LocalFileConnector){
					ImageIO.write(image, "jpg", fl.getAsFile());
					System.out.println("write a image");
				}else{
					File tmpImage = File.createTempFile(""+destination.hashCode(), ".jpg");
					ImageIO.write(image, "jpg", tmpImage);
					boolean flag = fl.copyFile1(tmpImage, true);
					if (flag) count++;
					else count1++;
					logger.info("[" + count + "]" +"[" + count1 + "]" 
					+ "DrawFeaturesOp Blot saved picture in " + destination);
				}
			}
		}
		return result;
	}

	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Frame> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


}
