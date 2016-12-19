package edu.fudan.lwang.codec;

import org.opencv.core.Mat;

public interface DecoderCallback {
	
	/**
	 * Description :
	 * 		send encoded data to the decoder
	 * 
	 * @param lastEncDataUsedSize
	 * @return
	 */
	public byte[] getEncodedData();
	
	/**
	 * Description :
	 * 		process need to be done after data is decoded
	 * <br>
	 * Notice:
	 * 		frame is in YUV format for H264 codec type
	 * @param frame
	 * @param lastEncDataUsedSize
	 */
	public void onDataDecoded(Mat frame, int dataUsed);
	
}
