package edu.fudan.lwang.codec;

import edu.fudan.stormcv.model.Frame;

public interface OperationHandler<E> {
    public boolean fillSourceBufferQueue(Frame frame);

    public E getDecodedData();

    public byte[] getEncodedData(E processedResult, String imageType);
}
