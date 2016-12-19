package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

public class DecoderFactory {
	public static DecoderWorker create(CodecType type) {
		switch (type) {
			case CODEC_TYPE_H264_CPU: {
				return new DecoderWorker(type);
			}
			// ------------- other types add blow ---------------
			default:
				return null;
		}
	}
}
