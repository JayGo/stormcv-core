package edu.fudan.lwang.codec;

import org.opencv.core.Mat;

public interface EncoderCallback {
	
	/**
	 * Description :
	 * 		process need to be done before data encoded
	 * @param frame
	 * @return processed mat in YUV format
	 */
	public Mat beforeDataEncoded(Mat frame);
	
	/**
	 * Description :
	 * 		process need to be done after data encoded
	 * @param encodedData
	 */
	public void onDataEncoded(byte[] encodedData);
	
	/**
	 * Description :
	 * 		process need to be done when encoder closed
	 */
	public void onEncoderClosed();
}
