package edu.fudan.lwang.codec;

import java.io.Serializable;

import edu.fudan.stormcv.model.Frame;

public interface OperationHandler<E>  extends Serializable{
    public boolean fillSourceBufferQueue(Frame frame);

    public E getDecodedData();

    public byte[] getEncodedData(E processedResult, String imageType);
}
