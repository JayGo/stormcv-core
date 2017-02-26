package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

public class EncoderFactory {
    public static EncoderWorker create(CodecType type) {
        switch (type) {
            case CODEC_TYPE_H264_CPU: {
                return new EncoderWorker(type);
            }
            // ------------- other types add blow ---------------
            default:
                return null;
        }
    }
}
