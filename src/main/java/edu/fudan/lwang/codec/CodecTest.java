package edu.fudan.lwang.codec;

import edu.fudan.lwang.codec.Common.CodecType;

public class CodecTest {
	public static void  main (String [] args) {
		
		System.load("/usr/local/opencv/share/OpenCV/java/libopencv_java2413.so");
		System.load("/usr/local/LwangCodec/lib/libHgCodec.so");
		
		SourceInfo mSourceInfo = Codec.fetchSourceInfo("/home/jliu/Videos/grass1080.mp4", "12345678", CodecType.CODEC_TYPE_H264_CPU);
		
		System.out.println(mSourceInfo);
		//************* this block is a test of lwang's encoder and decoder ********
		Codec.startEncodeToFrameQueue(mSourceInfo);
				
		String decoderQueueId = mSourceInfo.getEncodeQueueId()+"_decoder";
		MatQueueManager.getInstance().registerQueue(decoderQueueId);
		Codec.registerDecoder(mSourceInfo, mSourceInfo.getEncodeQueueId(), decoderQueueId);
		// ************************* end of block **********************************
	}
}
