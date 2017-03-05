package edu.fudan.stormcv.model;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/3/17 - 5:48 AM
 * Description:
 */
public class ImageRequest {
    private String streamId;
    private String inputLocation;
    private String outputLocation;
    private int processType;

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getInputLocation() {
        return inputLocation;
    }

    public void setInputLocation(String inputLocation) {
        this.inputLocation = inputLocation;
    }

    public String getOutputLocation() {
        return outputLocation;
    }

    public void setOutputLocation(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    public int getProcessType() {
        return processType;
    }

    public void setProcessType(int processType) {
        this.processType = processType;
    }

    public ImageRequest(String streamId, String inputLocation, String outputLocation, int processType) {
        this.streamId = streamId;
        this.inputLocation = inputLocation;
        this.outputLocation = outputLocation;
        this.processType = processType;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("streamId:").append(streamId).append('\n');
        builder.append("inputLocation:").append(inputLocation).append('\n');
        builder.append("outputLocation:").append(outputLocation).append('\n');
        builder.append("processType:").append(processType).append('\n');
        return builder.toString();
    }
}
