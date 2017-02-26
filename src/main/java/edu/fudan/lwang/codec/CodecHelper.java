package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

public class CodecHelper {

    private static CodecHelper mCodecHelper;

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
     *
     * @param encoderId,   unique encoder ID refers to certain encoder
     * @param codecType,   encoder type
     * @param frameWidth,  width of frame
     * @param frameHeight, height of frame
     * @return, registration result
     */
    public native int registerEncoder(String encoderId, int codecType, int frameWidth, int frameHeight);

    /**
     * Description :
     * register a decoder
     *
     * @param decoderId, unique decoder ID refers to certain decoder
     * @param codecType, decoder type
     * @return, registration result
     */
    public native int registerDecoder(String decoderId, int codecType);

    /**
     * Description :
     * encode a frame
     *
     * @param encoderId, specify which encoder to use for encoding
     * @param matAddr,   origin mat address
     * @return, encoded data or null
     */
    public native byte[] encodeFrame(String encoderId, long matAddr);

    /**
     * Description :
     * decode a frame
     *
     * @param decoderId,  specify which decoder to use for decoding
     * @param encodeData, encoded data
     * @param results     results[0] ----> frame nums decoded
     *                    results[1] ----> how many encode_data does decoder used
     * @param yuvMatAddr, yuvMat send to decode, must specify type info
     */
    public native void decodeFrame(String decoderId, byte[] encodeData, int[] results, long yuvMatAddr);

    /**
     * Description :
     * release a encoder
     *
     * @param encoderId, specify which encoder to release
     */
    public native void releaseEncoder(String encoderId);

    /**
     * Description :
     * release a decoder
     *
     * @param decoderId, specify which decoder to release
     */
    public native void releaseDecoder(String decoderId);
}
