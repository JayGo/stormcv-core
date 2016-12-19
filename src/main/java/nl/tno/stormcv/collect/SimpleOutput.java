package nl.tno.stormcv.collect;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.operation.MjpegStreamingOp;

public class SimpleOutput implements Runnable {
	
//	private static final long serialVersionUID = -7089471372136887079L;

	public SimpleOutput() {
	}
	
//	@SuppressWarnings("restriction")
//	@SuppressWarnings("restriction")
	@Override
	public void run() {
		// TODO Auto-generated method stub
//		framerate(25);
//		ApplicationAdapter connector = new ApplicationAdapter(new SimpleOutput());
//		images = MjpegStreamingOp.getImages();
//		try {
//			server = HttpServerFactory.create ("http://localhost:"+port+"/", connector);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		server.start();
		while(true) {
			Frame frame = ReceiverQueue.getInstance().popNext();
			
			if(frame == null) {
				continue;
			}
			System.out.println("output frame : " + frame.getSequenceNr());
//			images.put(frame.getStreamId(), frame.getImage());
		}
	}
	
}
