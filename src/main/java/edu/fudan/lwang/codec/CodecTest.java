package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;
import edu.fudan.stormcv.util.LibLoader;

public class CodecTest {
    public static void main(String[] args) {
        LibLoader.loadHgCodecLib();
        LibLoader.loadOpenCVLib();

        SourceInfo mSourceInfo = Codec.fetchSourceInfo("/home/jliu/Videos/grass1080.mp4", "12345678", CodecType.CODEC_TYPE_H264_CPU);
        System.out.println(mSourceInfo);
        //************* this block is a test of lwang's encoder and decoder ********
        Codec.startEncodeToFrameQueue(mSourceInfo);

        String decoderQueueId = mSourceInfo.getSourceId() + "_decoder";
        MatQueueManager.getInstance().registerQueue(decoderQueueId);
        Codec.registerDecoder(mSourceInfo, mSourceInfo.getSourceId(), decoderQueueId);
        // ************************* end of block **********************************
    }
}
