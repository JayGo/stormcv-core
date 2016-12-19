package edu.fudan.lwang.codec;

import org.opencv.core.Mat;


import nl.tno.stormcv.model.Frame;

public interface OperationHandler {
	public boolean fillSourceBufferQueue(Frame frame);
	public Mat getMat();
}
