package edu.fudan.stormcv.service.model;

import edu.fudan.stormcv.constant.TopologyStatus;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 8:07 PM
 * Description:
 */
public class TopologyBasicInfo {
    private String topoName;
    private String topoId;
    private int workerNum;
    private String owner;
    private int uptimeSecs;
    private int taskNum;
    private int executorNum;
    private TopologyStatus status;

    public TopologyBasicInfo() {}

    public TopologyBasicInfo(String topoName, String topoId, int workerNum, String owner, int uptimeSecs, int taskNum, int executorNum, String status) {
        this.topoName = topoName;
        this.topoId = topoId;
        this.workerNum = workerNum;
        this.owner = owner;
        this.uptimeSecs = uptimeSecs;
        this.taskNum = taskNum;
        this.executorNum = executorNum;
        this.status = TopologyStatus.valueOf(status.toUpperCase());
    }

    public String getTopoName() {
        return topoName;
    }

    public void setTopoName(String topoName) {
        this.topoName = topoName;
    }

    public String getTopoId() {
        return topoId;
    }

    public void setTopoId(String topoId) {
        this.topoId = topoId;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(int workerNum) {
        this.workerNum = workerNum;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getUptimeSecs() {
        return uptimeSecs;
    }

    public void setUptimeSecs(int uptimeSecs) {
        this.uptimeSecs = uptimeSecs;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public int getExecutorNum() {
        return executorNum;
    }

    public void setExecutorNum(int executorNum) {
        this.executorNum = executorNum;
    }

    public TopologyStatus getStatus() {
        return status;
    }

    public void setStatus(TopologyStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "TopologyBasicInfo{" +
                "topoName='" + topoName + '\'' +
                ", topoId='" + topoId + '\'' +
                ", workerNum=" + workerNum +
                ", owner='" + owner + '\'' +
                ", uptime='" + uptimeSecs + '\'' +
                ", taskNum=" + taskNum +
                ", executorNum=" + executorNum +
                ", status=" + status +
                '}';
    }
}
