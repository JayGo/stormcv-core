package edu.fudan.lwang.codec;

public class NativeHelper {

	public static void loadLibraries() {
		System.load("/home/lwang/workspace_java/HgCodecJNI/lib/linux64_opencv_java248.so");
		System.load("/home/lwang/workspace_java/HgCodecJNI/lib/libhg_codec.so");
	}
	
}
