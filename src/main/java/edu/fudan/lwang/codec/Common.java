package edu.fudan.lwang.codec;

public class Common {

	public static final int CODEC_VERSION = 10;
	
	/**
	 * =======================================================
	 * Codec type in Java Code
	 * =======================================================
	 */
	public static enum CodecType {
		CODEC_TYPE_H264_CPU,
	}
	
	/**
	 * =======================================================
	 * Codec type for JNI transport
	 * =======================================================
	 */
	public static final int CODEC_TYPE_H264_CPU = 1;
	/**
	 * @deprecated
	 * not supported in version 1.0
	 */
	public static final int CODEC_TYPE_H264_GPU = 2;
	
	
	
	/**
	 * =======================================================
	 * CODEC Result for JNI transport
	 * =======================================================
	 */
	public static final int CODEC_OK = 1;
	public static final int CODEC_ERROR_QUEUE_FULL = 10;
	public static final int CODEC_ERROR_QUEUE_EMPTY = 11;

	public static final int CODEC_ERROR_NO_MEM_AVALIABLE = 20;

	public static final int CODEC_ERROR_FFMPEG_DECODER_CODEC_NOT_FIND = 30;
	public static final int CODEC_ERROR_FFMPEG_DECODER_CONTEXT_NOT_ALLOCATE = 31;
	public static final int CODEC_ERROR_FFMPEG_DECODER_CONTEXT_NOT_OPEN = 32;
	public static final int CODEC_ERROR_FFMPEG_DECODER_DECODE_FAILED = 33;

	public static final int CODEC_ERROR_FFMPEG_ENCODER_ENCODE_FAILED = 40;
}
