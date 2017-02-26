package nl.tno.stormcv.util.reader;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.tno.stormcv.model.Frame;

public class RawStreamReader extends Thread {
	private Logger logger = LoggerFactory.getLogger(XuggleStreamReader.class);

	private int dataLength;
	private byte[] buffer;
	private byte[] picData;
	private int numOfBufferBytes;
	private Socket socket;
	private InputStream in;
	private int TCPSIZE;
	private String location;

	// for stormcv
	private String streamId;
	private int frameSkip;
	private int groupSize;
	private long frameNr; // number of the frame read so far
	private boolean running = false; // indicator if the reader is still active
	private LinkedBlockingQueue<Frame> frameQueue; // queue used to store frames
	private long lastRead = -1; // used to determine if the EOF was reached if
								                  // Xuggler does not detect it
	private int sleepTime;
	private int frameMs;
	private String imageType = Frame.JPG_IMAGE;

	public RawStreamReader(String ip, int port, int bufferSize,
			String streamId, String imageType, int frameSkip, int groupSize,
			int sleepTime, LinkedBlockingQueue<Frame> frameQueue) {

		buffer = new byte[bufferSize];
		picData = new byte[4096000];
		dataLength = 0;
		TCPSIZE = 409600;
		location = ip + ":" + port;

		try {
			socket = new Socket(ip, port);
			in = socket.getInputStream();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// for stormcv
		this.imageType = imageType;
		this.frameSkip = Math.max(1, frameSkip);
		this.groupSize = Math.max(1, groupSize);
		this.sleepTime = sleepTime;
		this.frameQueue = frameQueue;
		this.streamId = "" + this.hashCode(); // give a default streamId
		lastRead = System.currentTimeMillis() + 10000;
		this.frameNr = 0;
		this.frameMs = 50;
	}

	private int toInt(byte[] b, boolean increase) {
		int l = 0;
		if (increase) {
			l = b[0] & 0xff;
			l |= (b[1] & 0xff) << 8;
			l |= (b[2] & 0xff) << 16;
			l |= (b[3] & 0xff) << 24;
		} else {
			l = b[3] & 0xff;
			l |= (b[2] & 0xff) << 8;
			l |= (b[1] & 0xff) << 16;
			l |= (b[0] & 0xff) << 24;
		}
		return l;
	}

	@Override
	public void run() {
		running = true;
		if (in == null) {
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}

		// int i = 0;
		// FileOutputStream fileOutputStream = null;
		logger.info("start to read stream from the given ip and port");
		while (!Thread.interrupted()) {
			lastRead = System.currentTimeMillis();
			try {
				while (dataLength == 0) {
					numOfBufferBytes += in.read(buffer, numOfBufferBytes,
							TCPSIZE);
					dataLength = toInt(buffer, true);
				}
				while (numOfBufferBytes < dataLength + 4) {
					numOfBufferBytes += in.read(buffer, numOfBufferBytes,
							TCPSIZE);
				}

				System.arraycopy(buffer, 4, picData, 0, dataLength);
				processFrame();

				/*
				 * String file = "pic_" + i + ".jpg";
				 * System.out.println("a frame received! + " + file);
				 * fileOutputStream = new FileOutputStream(file);
				 * fileOutputStream.write(picData); fileOutputStream.close();
				 * System.out.println(file + " have saved!");
				 */

				System.arraycopy(buffer, 4 + dataLength, buffer, 0,
						numOfBufferBytes - 4 - dataLength);
				numOfBufferBytes = numOfBufferBytes - 4 - dataLength;
				dataLength = 0;
			} catch (Exception e) {
				e.printStackTrace();
			}
			// ++i;
		}
		try {
			socket.close();
			running = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *  Process a Frame : new Frame and put it to queue
	 **/
	public void processFrame() {
		lastRead = System.currentTimeMillis();
		BufferedImage img = null;
		ByteArrayInputStream byteIn = new ByteArrayInputStream(picData);
		try {
			ImageInputStream imageIn = ImageIO.createImageInputStream(byteIn);
			img = ImageIO.read(imageIn);
			byteIn.close();
		} catch (Exception e) {
			logger.warn(
					"Unable to revert bytes to image due to: " + e.getMessage(),
					e);
		}

		if (frameNr % frameSkip < groupSize) {
			try {
				long timestamp = System.currentTimeMillis();
				if (frameMs > 0 ) timestamp = frameNr * frameMs;
				Frame newFrame = new Frame(streamId, frameNr, imageType,
						picData, timestamp, new Rectangle(0, 0, img.getWidth(),
								img.getHeight()));
				newFrame.getMetadata().put("uri", location);
				frameQueue.put(newFrame);
				// enforced throttling
				if (sleepTime > 0)
					Utils.sleep(sleepTime);
				// queue based throttling
				if (frameQueue.size() > 20)
					Utils.sleep(frameQueue.size());
			} catch (Exception e) {
				logger.warn(
						"Unable to process new frame due to: " + e.getMessage(),
						e);
			}
		}
		frameNr++;
	}
	
	/**
	 * Tells the RawStreamReader to stop reading frames
	 */
	public void stopStreamReader() {
		running = false;
	}
	
	/**
	 * Returns whether the RawStreamReader is still active or not
	 * @return
	 */
	public boolean isRunning() {
		// kill this thread if the last frame read is to long ago (means Xuggler missed the EoF) and clear resources 
		if(lastRead > 0 && System.currentTimeMillis() - lastRead > 3000) {
			running = false; 
			return this.running;
		}
		return true;
	}
}