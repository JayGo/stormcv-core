package nl.tno.stormcv.util;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.model.Frame;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;

/**
 * This class reads a video stream or file, decodes frames and puts those in a queue for further processing. 
 * The StreamReader will automatically throttle itself based on the size of the queue it writes the frames to.
 * This is done to avoid memory overload if production of frames is higher than the consumption. The actual decoding of 
 * frames is done by Xuggler which in turn uses FFMPEG (xuggler jar file is shipped with ffmpeg binaries).
 *  
 * @author Corne Versloot
 *
 */
public class StreamReader extends MediaListenerAdapter implements Runnable {
	
	private Logger logger = LoggerFactory.getLogger(StreamReader.class);
	private IMediaReader mediaReader;
    private int mVideoStreamIndex = -1;
	private String streamId;
	private int frameSkip;
	private int groupSize;
	private long frameNr; // number of the frame read so far
	private boolean running = false; // indicator if the reader is still active
	private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
	private long lastRead = -1; // used to determine if the EOF was reached if Xuggler does not detect it
	private int sleepTime;
	private boolean useSingleID = false;
	private String tmpDir;
	private int frameMs = -1;

	private String streamLocation;
	private LinkedBlockingQueue<String> videoList = null;
	private String imageType = Frame.JPG_IMAGE;
   
	  public StreamReader( LinkedBlockingQueue<String> videoList, String imageType, int frameSkip, int groupSize, int sleepTime, boolean uniqueIdPerFile, LinkedBlockingQueue<Frame> frameQueue){
		  this.videoList = videoList;
		  this.imageType = imageType;
		  this.frameSkip = Math.max(1, frameSkip);
		  this.groupSize = Math.max(1, groupSize);
		  this.sleepTime = sleepTime;
		  this.frameQueue = frameQueue;
		  this.useSingleID = uniqueIdPerFile;
		  this.streamId = ""+this.hashCode(); // give a default streamId
		  lastRead = System.currentTimeMillis()+10000;
		  try {
			tmpDir = File.createTempFile("abc", ".tmp").getParent();
		  } catch (IOException e) {	}
	  }
	
	  public StreamReader( String streamId, String streamLocation, String imageType, int frameSkip, int groupSize, int sleepTime, LinkedBlockingQueue<Frame> frameQueue){
		  this.streamLocation = streamLocation;
		  this.imageType = imageType;
		  this.frameSkip = Math.max(1, frameSkip);
		  this.groupSize = Math.max(1, groupSize);
		  this.sleepTime = sleepTime;
		  this.frameQueue = frameQueue;
		  this.streamId = streamId;
		  lastRead = System.currentTimeMillis()+10000;
		  try {
				tmpDir = File.createTempFile("abc", ".tmp").getParent();
		  } catch (IOException e) {	}
	  }
	  
    /**
     * Start reading the provided URL
     * @param url the url to read video from
     * @param frameskip milliseconds between frames to extract
     */
	@Override
	public void run(){
		running = true;
		while(running){
			try{
				// if a url was provided read it
				if(videoList == null && streamLocation != null){
					logger.info("Start reading stream: "+streamLocation);
					runReader();
//					mediaReader = ToolFactory.makeReader(streamLocation);
//					mediaReader.getContainer().setForcedVideoCodec(ICodec.ID.CODEC_ID_H264);
//					mediaReader.getContainer().setInputBufferLength(100);
//					//mediaReader.getContainer().setForcedVideoCodec(ICodec.ID.CODEC_ID_MPEG4);
				}else if(videoList != null){
					// read next video from the list or block until one is available
					logger.info("Waiting for new file to be downloaded...");
					streamLocation = videoList.take();
					
					if(!useSingleID){
						streamId = ""+streamLocation.hashCode();
						if(streamLocation.contains("/")) streamId = streamLocation.substring(streamLocation.lastIndexOf('/')+1)+"_"+streamId;
					}
					logger.info("Start reading File: "+streamLocation);
					
					// read framerate from file
			        IContainer cont = IContainer.make();
			        if (cont.open(streamLocation, IContainer.Type.READ, null) > 0) try{
				        for(int s=0; s < cont.getNumStreams(); s++){// find the videostream
				        	if(cont.getStream(s).getStreamCoder().getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO){
				        		frameMs = (int)Math.floor(1000f/cont.getStream(s).getStreamCoder().getFrameRate().getDouble());
				        	}
				        }
			        }catch(Exception e){
			        	logger.warn("Unable to read metadata from video");
			        }
			        
					mediaReader = ToolFactory.makeReader(streamLocation);
				} else {
					logger.error("No video list or url provided, nothing to read");
					break;
				}
				lastRead = System.currentTimeMillis() + 10000;
	
				mVideoStreamIndex = -1;
				if(!useSingleID) frameNr = 0;
		        mediaReader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
		        mediaReader.addListener(this);
		        
		        while (mediaReader.readPacket() == null && running ) ;
		 
		        // reset internal state
		        mediaReader.close();
		        // delete the local file (if it is in tmp dir)
		        if(videoList != null){
		        	File localFile = new File(streamLocation);
		        	if(localFile.getAbsolutePath().startsWith(tmpDir)){
		        		localFile.delete();
		        	}
		        }
			}catch(Exception e){
				logger.warn("Stream closed unexpectatly: "+e.getMessage(), e);
				// sleep a minute and try to read the stream again
				Utils.sleep(1 * 10 * 1000);  // have change to 10s for test
			}
		}
		
        mVideoStreamIndex = -1;
        running = false;
	}
	
	/**
	 * Gets called when FFMPEG transcoded a frame
	 */
	@Override
	public void onVideoPicture(IVideoPictureEvent event) {
		lastRead  = System.currentTimeMillis();
        if (event.getStreamIndex() != mVideoStreamIndex) {
            if (mVideoStreamIndex == -1){
                mVideoStreamIndex = event.getStreamIndex();
            }else return;
        }
        if(frameNr % frameSkip < groupSize) try{
        	BufferedImage frame = event.getImage();
        	byte[] buffer = ImageUtils.imageToBytes(frame, imageType);
        	long timestamp = event.getTimeStamp(TimeUnit.MILLISECONDS);
        	if(frameMs > 0 ) timestamp = frameNr * frameMs;
        	Frame newFrame = new Frame(streamId, frameNr, imageType, buffer, timestamp, new Rectangle(0, 0,frame.getWidth(), frame.getHeight()));
        	newFrame.getMetadata().put("uri", streamLocation);
        	frameQueue.put(newFrame);
          // logger.info("read one frame at " + lastRead);
        	// enforced throttling
        	if(sleepTime > 0) Utils.sleep(sleepTime);
        	// queue based throttling 
        	if(frameQueue.size() > 20) Utils.sleep(frameQueue.size());
        }catch(Exception e){
        	logger.warn("Unable to process new frame due to: "+e.getMessage(), e);
        }
        
        frameNr++;
    }

	/**
	 * Tells the StreamReader to stop reading frames
	 */
	public void stop(){
		running = false;
	}
	
	/**
	 * Returns whether the StreamReader is still active or not
	 * @return
	 */
	public boolean isRunning(){
		// kill this thread if the last frame read is to long ago (means Xuggler missed the EoF) and clear resources 
		if(lastRead > 0 && System.currentTimeMillis() - lastRead > 3000){
			System.out.println("enter in running");
			running = false;
			if(mediaReader != null && mediaReader.getContainer() != null) mediaReader.getContainer().close();
			return this.running;
		}
		return true;
	}
	
	public void runReader() {
		while (true) {
		IContainer container = IContainer.make();
		if (container.open(streamLocation, IContainer.Type.READ, null) < 0)
	            throw new IllegalArgumentException("could not open file: " + streamLocation);
		int numStreams = container.getNumStreams();
		int videoStreamId = -1;
        IStreamCoder videoCoder = null;
        for(int i = 0; i < numStreams; i++)
        {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO)
            {
                videoStreamId = i;
                videoCoder = coder;
                break;
            }
        }
        if (videoStreamId == -1)
            throw new RuntimeException("could not find video stream in container: "
                    +streamLocation);
        
        if (videoCoder.open() < 0)
            throw new RuntimeException("could not open video decoder for container: "
                    +streamLocation);

        IPacket packet = IPacket.make();
        long firstTimestampInStream = Global.NO_PTS;
        long systemClockStartTime = 0;
        while(container.readNextPacket(packet) >= 0 && running)
        {
            if (packet.getStreamIndex() == videoStreamId)
            {
                IVideoPicture picture = IVideoPicture.make(videoCoder.getPixelType(),
                        videoCoder.getWidth(), videoCoder.getHeight());
                int offset = 0;
                while(offset < packet.getSize())
                {
                    int bytesDecoded = videoCoder.decodeVideo(picture, packet, offset);
                    if (bytesDecoded < 0)
                        throw new RuntimeException("got error decoding video in: "
                                + streamLocation);
                    offset += bytesDecoded;

                    if (picture.isComplete())
                    {
                        IVideoPicture newPic = picture;
                        if (firstTimestampInStream == Global.NO_PTS)
                        {
                            firstTimestampInStream = picture.getTimeStamp();
                            systemClockStartTime = System.currentTimeMillis();
                        } else {
                            long systemClockCurrentTime = System.currentTimeMillis();
                            long millisecondsClockTimeSinceStartofVideo =
                                    systemClockCurrentTime - systemClockStartTime;
                            // compute how long for this frame since the first frame in the
                            // stream.
                            // remember that IVideoPicture and IAudioSamples timestamps are
                            // always in MICROSECONDS,
                            // so we divide by 1000 to get milliseconds.
                            long millisecondsStreamTimeSinceStartOfVideo =
                                    (picture.getTimeStamp() - firstTimestampInStream)/1000;
                            final long millisecondsTolerance = 50; // and we give ourselfs 50 ms of tolerance
                            final long millisecondsToSleep =
                                    (millisecondsStreamTimeSinceStartOfVideo -
                                            (millisecondsClockTimeSinceStartofVideo +
                                                    millisecondsTolerance));
                            if (millisecondsToSleep > 0)
                            {
                                try
                                {
                                    Thread.sleep(millisecondsToSleep);
                                }
                                catch (InterruptedException e)
                                {
                                    // we might get this when the user closes the dialog box, so
                                    // just return from the method.
                                    return;
                                }
                            }
                        }                        
                        BufferedImage frame = com.xuggle.xuggler.Utils.videoPictureToImage(newPic);         	
                    	
                    	
                    	lastRead  = System.currentTimeMillis();
                        if(frameNr % frameSkip < groupSize) try{
                        	byte[] buffer = ImageUtils.imageToBytes(frame, imageType);
                        	long timestamp = System.currentTimeMillis();
                        	if(frameMs > 0 ) timestamp = frameNr * frameMs;
                        	//long timestamp = System.currentTimeMillis();
                        	Frame newFrame = new Frame(streamId,frameNr, imageType, buffer, timestamp, new Rectangle(0, 0,frame.getWidth(), frame.getHeight()));
                        	newFrame.getMetadata().put("uri",streamLocation);
                        	frameQueue.put(newFrame);
                        	// logger.info("read one frame at " + lastRead);
                        	// enforced throttling
                        	if(sleepTime > 0) Utils.sleep(sleepTime);
                        	// queue based throttling 
                        	if(frameQueue.size() > 20) Utils.sleep(frameQueue.size());
                        }catch(Exception e){
                        	logger.warn("Unable to process new frame due to: "+e.getMessage(), e);
                        }
                        
                        frameNr++;
                    }
                }
            }
            else
            {
                do {} while(false);
            }

        }
        if (videoCoder != null)
        {
            videoCoder.close();
            videoCoder = null;
        }
        if (container !=null)
        {
            container.close();
            container = null;
        }
    }
	
	}
}
	
