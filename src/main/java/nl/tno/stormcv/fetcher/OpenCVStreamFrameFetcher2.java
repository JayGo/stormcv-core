package nl.tno.stormcv.fetcher;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.GroupOfFrames;
import nl.tno.stormcv.model.MatImage;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.model.serializer.GroupOfFramesSerializer;
import nl.tno.stormcv.model.serializer.MatImageSerializer;
import nl.tno.stormcv.operation.GroupOfFramesOp;
import nl.tno.stormcv.util.Constant;
import nl.tno.stormcv.util.OpenCVStreamReader2;

import org.apache.storm.task.TopologyContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link IFetcher} implementation that reads video streams (either live or not). The StreamFrameFetcher is initialized
 * with a set of url's it must read. These url's are divided among all StreamFrameFetchers in the topology. So if the number of
 * urls is larger than then number of Fetchers some of them will read and decode multiple streams in parallel. Note that this can become
 * a problem if the number of streams read by the single spout consumes to many resources (network and/or cpu).
 * 
 * The frameSkip and groupSize parameters define which frames will be extracted using: frameNr % frameSkip < groupSize-1
 * With a frameSkip of 10 and groupSize of 2 the following framenumbers will be extracted: 0,1,10,11,20,21,.. Both frameskip and
 * groupSize have default value 1 which means that all frames are read.
 * 
 * It is possible to provide an additional sleep which is enforced after each emitted frame. This sleep can be used to throttle the StreamFrameFetcher
 * when it is reading streams to fast (i.e. faster than topology can process). Use of the sleep should be avoided when possible and throttling of the topology
 * should be done using the MAX_SPOUT_PENDING configuration parameter.
 * 
 * This fetcher can be configured to emit {@link GroupOfFrames} objects instead of {@link Frame} by using the groupOfFramesOutput method. Emitting
 * a {@link GroupOfFrames} can be useful when the subsequent Operation requires multiple frames of the same stream in which case the
 * {@link GroupOfFramesOp} can be used without a batcher. 
 * 
 * @author Corne Versloot
 *
 */
public class OpenCVStreamFrameFetcher2 implements IFetcher<CVParticle> {

	private static final long serialVersionUID = 7135270229614102711L;
	protected List<String> locations;
	protected int frameSkip = 1;
	private int groupSize = 1;
	//protected LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(Constant.SpoutQueueSize);
	protected LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(Constant.SpoutQueueSize);
	protected LinkedBlockingQueue<MatImage> matQueue = new LinkedBlockingQueue<MatImage>(Constant.SpoutQueueSize);
	private LinkedList<MatImage> matCache = new LinkedList<>();
	protected Map<String, OpenCVStreamReader2> streamReaders;
	private int sleepTime = 0;
	private String imageType;
	private int batchSize = 1;
	private List<Frame> frameGroup;
	private String id;

	public OpenCVStreamFrameFetcher2(List<String> locations){
//		try {
//			NativeUtils.loadOpencvLib();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		this.locations = locations;
	}
	
	public OpenCVStreamFrameFetcher2 frameSkip(int skip){
		this.frameSkip = skip;
		return this;
	}
	
	/**
	 * Sets the number of frames for a group
	 * @param size
	 * @return
	 */
	public OpenCVStreamFrameFetcher2 groupSize(int size) {
		this.groupSize  = size;
		return this;
	}
	
	public OpenCVStreamFrameFetcher2 sleep(int ms) {
		this.sleepTime = ms;
		return this;
	}
	
	/**
	 * Specifies the number of frames to be send at once. If set to 1 (default value) this Fetcher will emit
	 * {@link Frame} objects. If set to 2 or more it will emit {@link GroupOfFrames} objects.
	 * @param nrFrames
	 * @return
	 */
	public OpenCVStreamFrameFetcher2 groupOfFramesOutput(int nrFrames){
		this.batchSize = nrFrames;
		return this;
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map conf, TopologyContext context) throws Exception {
		this.id = context.getThisComponentId();
		int nrTasks = context.getComponentTasks(id).size();
		int taskIndex = context.getThisTaskIndex();
		
		if(conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)){
			imageType = (String)conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
		}
		
		// change the list based on the number of tasks working on it
		if(this.locations != null && this.locations.size() > 0){
			int batchSize = (int) Math.floor(locations.size() / nrTasks);
			int start = batchSize * taskIndex;
			locations = locations.subList(start, Math.min(start + batchSize, locations.size()));
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public CVParticleSerializer getSerializer() {
		if(batchSize  <= 1) return new FrameSerializer();
		else return new GroupOfFramesSerializer();
	}

	public MatImageSerializer getMatImageSerializer() {
		return new MatImageSerializer();
	}

	@Override
	public void activate() {
		if(streamReaders != null){
			this.deactivate();
		}
		streamReaders = new HashMap<String, OpenCVStreamReader2>();
		for(String location : locations){
			
			String streamId = ""+location.hashCode();
			if(location.contains("/")){
				streamId = id+"_"+location.substring(location.lastIndexOf("/")+1) + "_" + streamId;
			}
			OpenCVStreamReader2 reader = new OpenCVStreamReader2(streamId, location, imageType, frameSkip, groupSize, sleepTime, frameQueue, matQueue);
			streamReaders.put(location, reader);
			new Thread(reader).start();
		}
	}

	@Override
	public void deactivate() {
		if(streamReaders != null) for(String location : streamReaders.keySet()){
			streamReaders.get(location).stop();
		}
		streamReaders = null;
	}

	@Override
	public CVParticle fetchData() {
		if(streamReaders == null) this.activate();
		Frame frame = frameQueue.poll();
		if(frame != null) {
			if(batchSize <= 1){
				return frame;
			}else{
				if(frameGroup == null || frameGroup.size() >= batchSize) frameGroup = new ArrayList<Frame>();
				frameGroup.add(frame);
				if(frameGroup.size() == batchSize){
					return new GroupOfFrames(frameGroup.get(0).getStreamId(), frameGroup.get(0).getSequenceNr(), frameGroup);
				}
			}
		}
		return null;
	}
	
//	@Override
//	public CVParticle fetchData() {
//		if (streamReaders == null)
//			this.activate();
//		if (matCache.isEmpty()) {
//			matQueue.drainTo(matCache);
//		}
//		if (matQueue.size() > 0) {
//			MatImage matImage = matCache.remove(0);
//			Frame frame = null;
//			try {
//				frame = matImage.convertToFrame();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//
//			if (frame != null) {
//				if (batchSize <= 1) {
//					return frame;
//				} else {
//					if (frameGroup == null || frameGroup.size() >= batchSize)
//						frameGroup = new ArrayList<Frame>();
//					frameGroup.add(frame);
//					if (frameGroup.size() == batchSize) {
//						return new GroupOfFrames(frameGroup.get(0)
//								.getStreamId(), frameGroup.get(0)
//								.getSequenceNr(), frameGroup);
//					}
//				}
//			} else {
//				System.out.println("frame is null at frame " + frame.getSequenceNr());
//			}
//		}
//		return null;
//	}
	

	public MatImage fetchData2() {
		if (streamReaders == null)
			this.activate();
		MatImage matImage = matQueue.poll();
		if (matImage != null) {
			return matImage;
		}
//		if (matCache.isEmpty()) {
//			matQueue.drainTo(matCache);
//		}
//		if (matQueue.size() > 0) {
//			MatImage matImage = matCache.remove(0);
//			return matImage;
//		}
		return null;
	}
}
