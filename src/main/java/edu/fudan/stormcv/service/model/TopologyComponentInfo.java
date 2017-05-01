package edu.fudan.stormcv.service.model;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 8:58 PM
 * Description:
 */
public class TopologyComponentInfo {
    private String topoName;
    private String componentId;
    private String type;
    private int executorNum;
    private int taskNum;
    private long allTimeProcessed;
    private long allTimeFailed;
    private double allTimeLatency;

    public TopologyComponentInfo() {

    }

    public TopologyComponentInfo(String topoName, String componentId, String type, int executorNum, int taskNum, long allTimeProcessed, long allTimeFailed, double allTimeLatency) {
        this.topoName = topoName;
        this.componentId = componentId;
        this.type = type;
        this.executorNum = executorNum;
        this.taskNum = taskNum;
        this.allTimeProcessed = allTimeProcessed;
        this.allTimeFailed = allTimeFailed;
        this.allTimeLatency = allTimeLatency;
    }

    public String getTopoName() {
        return topoName;
    }

    public void setTopoName(String topoName) {
        this.topoName = topoName;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getExecutorNum() {
        return executorNum;
    }

    public void setExecutorNum(int executorNum) {
        this.executorNum = executorNum;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public long getAllTimeProcessed() {
        return allTimeProcessed;
    }

    public void setAllTimeProcessed(long allTimeProcessed) {
        this.allTimeProcessed = allTimeProcessed;
    }

    public long getAllTimeFailed() {
        return allTimeFailed;
    }

    public void setAllTimeFailed(long allTimeFailed) {
        this.allTimeFailed = allTimeFailed;
    }

    public double getAllTimeLatency() {
        return allTimeLatency;
    }

    public void setAllTimeLatency(double allTimeLatency) {
        this.allTimeLatency = allTimeLatency;
    }

    @Override
    public String toString() {
        return "TopologyComponentInfo{" +
                "topoName='" + topoName + '\'' +
                ", componentId='" + componentId + '\'' +
                ", type='" + type + "\'" +
                ", executorNum=" + executorNum +
                ", taskNum=" + taskNum +
                ", allTimeAcked=" + allTimeProcessed +
                ", allTimeFailed=" + allTimeFailed +
                ", allTimeExecuteLatency=" + allTimeLatency +
                '}';
    }
}
