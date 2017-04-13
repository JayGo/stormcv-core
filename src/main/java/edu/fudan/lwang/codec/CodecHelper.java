package edu.fudan.lwang.codec;

import org.apache.log4j.Logger;

import edu.fudan.lwang.codec.Common.CodecType;

public class CodecHelper {

    private static CodecHelper mCodecHelper;
    private final static Logger logger = Logger.getLogger(CodecHelper.class);

    private CodecHelper() {

    }

    public synchronized static CodecHelper getInstance() {
        if (mCodecHelper == null) {
            mCodecHelper = new CodecHelper();
        }
        return mCodecHelper;
    }

    public int getCodecTypeInt(CodecType codecType) {

        int codecTypeInt = Common.CODEC_TYPE_H264_CPU;
        
//        logger.info("============codecType:"+codecType);
        
        switch (codecType) {
            case CODEC_TYPE_H264_CPU:
                codecTypeInt = Common.CODEC_TYPE_H264_CPU;
                break;
        }
        return codecTypeInt;
    }

	/**
	 * Description :
	 * register a encoder
	 * @param encoderId, unique encoder ID refers to certain encoder
	 * @param codecType, encoder type
	 * @param buf_mat_nativeObj, frame nativeObj
	 * @param params_mat_nativeObj, params Mat nativeObj
	 * @return, registration result
	 */
	public native int registerEncoder(String encoderId, int codecType, long bufMatNativeObj, long paramsMatNativeObj);
	
	/**
	 * Description :
	 * register a decoder
	 * @param decoderId, unique decoder ID refers to certain decoder
	 * @param codecType, decoder type
	 * @return, registration result
	 */
	public native int registerDecoder(String decoderId, int codecType, int width, int height, long paramsMatNativeObj);
	
	/**
	 * Description :
	 * encode a frame
	 * @param encoderId, specify which encoder to use for encoding
	 * @param matAddr, origin mat address
	 * @return, encoded data or null 
	 */
	public native byte[] encodeFrame(String encoderId, long matAddr);
	
	/**
	 * Description :
	 * decode a frame
	 * @param decoderId, specify which decoder to use for decoding
	 * @param encodeData, encoded data
	 * @param results
	 * 		results[0] ----> frame nums decoded
	 * 		results[1] ----> how many encode_data does decoder used
	 * @param matAddr, mat send to decode, must specify type info
	 */
	public native int decodeFrame(String decoderId, byte[] encodeData, int[] results, long matAddr);
	
	/**
	 * Description :
	 * release a encoder
	 * @param encoderId, specify which encoder to release
	 */
	public native void releaseEncoder(String encoderId);
	
	/**
	 * Description :
	 * release a decoder
	 * @param decoderId, specify which decoder to release
	 */
	public native void releaseDecoder(String decoderId);
}
