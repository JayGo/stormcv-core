package nl.tno.stormcv.util;

public class StreamerHelper {
	private static StreamerHelper instance = null;
	
	private StreamerHelper() {
		
	}
	
	public synchronized static StreamerHelper getInstance() {
		if(instance == null) {
			instance = new StreamerHelper();
		}
		return instance;
	}
	
	public native boolean sendFirstFrame(byte [] data);
	
	public native boolean sendNormalFrame(byte [] data);
	
	public native boolean connectToRtmp(String rtmpAddr);
}
