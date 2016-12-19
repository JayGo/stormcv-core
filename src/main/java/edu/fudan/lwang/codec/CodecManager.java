package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

import java.util.HashMap;
import java.util.Map;

import org.opencv.highgui.VideoCapture;

import com.amazonaws.services.elasticbeanstalk.model.transform.RetrieveEnvironmentInfoResultStaxUnmarshaller;


public class CodecManager {

	private static CodecManager mCodecManager;
	private Map<String,EncoderWorker> encoders;
	private Map<String,DecoderWorker> decoders;
	
	private CodecManager() {
		encoders = new HashMap<String,EncoderWorker>();
		decoders = new HashMap<String,DecoderWorker>();
	}
	
	public synchronized static CodecManager getInstance() {
		if(mCodecManager == null) {
			mCodecManager = new CodecManager();
		}
		return mCodecManager;
	}
	
	/**
	 * Description :
	 * 		register a encoder
	 * @param encoderId, unique encoder ID refers to certain encoder
	 * @param codecType, encoder type
	 * @param frameWidth, width of frame
	 * @param frameHeight, height of frame
	 * @param mEncoderCallback, encoder callback
	 * @return, registration result
	 */
	public int registerEncoder(EncoderWorker encoderWorker) {
		encoders.put(encoderWorker.getEncoderId(), encoderWorker);
		return encoderWorker.registerEncoder();
	}
	
	public EncoderWorker getEncoder(String encoderId) {
		return encoders.get(encoderId);
	}
	
	/**
	 *  Description :
	 * 		register a decoder
	 * @param encoderId, unique encoder ID refers to certain encoder
	 * @param codecType, encoder type
	 * @param mDecoderCallback, decoder callback
	 * @return
	 */
	public int registerDecoder(DecoderWorker decoderWorker) {
		decoders.put(decoderWorker.getDecoderId(), decoderWorker);
		return decoderWorker.registerDecoder();
	}
	
	public DecoderWorker getDecoder(String decoderId) {
		return decoders.get(decoderId);
	}
	
	/**
	 * Description :
	 * 		start the encoder according to the encoderId
	 * @param encoderId, unique encoder ID refers to certain encoder
	 */
	public void startEncode(String encoderId) {
		EncoderWorker mEncoderWorker = encoders.get(encoderId);
		mEncoderWorker.start();
	}
	
	/**
	 * Description :
	 * 		start the decoder according to the decoderId
	 * @param decoderId, unique decoder ID refers to certain decoder
	 */
	public void startDecode(String decoderId) {
		DecoderWorker mDecoderWorker = decoders.get(decoderId);
		mDecoderWorker.start();
	}
	
	/**
	 * Description :
	 * 		stop the encoder according to the encoderId
	 * @param encoderId, unique encoder ID refers to certain encoder
	 */
	public void stopEncode(String encoderId) {
		EncoderWorker mEncoderWorker = encoders.get(encoderId);
		mEncoderWorker.releaseEncoder();
		mEncoderWorker.interrupt();
		encoders.remove(encoderId);
	}
	
	/**
	 * Description :
	 * 		stop the decoder according to the decoderId
	 * @param decoderId, unique decoder ID refers to certain decoder
	 */
	public void stopDecode(String decoderId) {
		DecoderWorker mDecoderWorker = decoders.get(decoderId);
		mDecoderWorker.releaseDecoder();
		mDecoderWorker.interrupt();
		decoders.remove(decoderId);
	}
	
	/**
	 * Description :
	 * 		is encoder alive
	 * @param codecId
	 * @return
	 */
	public boolean isEncoderAlive(String encoderId) {
		EncoderWorker mEncoderWorker = encoders.get(encoderId);
		return mEncoderWorker.isAlive();
	}
	
	/**
	 * Description :
	 * 		is encoder alive
	 * @param codecId
	 * @return
	 */
	public boolean isDecoderAlive(String decoderId) {
		DecoderWorker mDecoderWorker = decoders.get(decoderId);
		return mDecoderWorker.isAlive();
	}
	
	/**
	 * Description :
	 * 		encoder join
	 * @param encoderId
	 * @throws InterruptedException
	 */
	public void encoderJoin(String encoderId) throws InterruptedException {
		EncoderWorker mEncoderWorker = encoders.get(encoderId);
		mEncoderWorker.join();
	}
	
	/**
	 * Description :
	 * 		decoder join
	 * @param encoderId
	 * @throws InterruptedException
	 */
	public void decoderJoin(String decoderId) throws InterruptedException {
		DecoderWorker mDecoderWorker = decoders.get(decoderId);
		mDecoderWorker.join();
	}
}
