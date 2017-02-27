package edu.fudan.stormcv.util;

/**
 * A simple c++ interface created by jliu to implements foreground extraction of a image
 *
 * @author jkyan, jliu
 */

public class ForegroundExtractionJNI {

    public static native byte[] foregroundExtract(byte[] inputData, int height, int width);

}