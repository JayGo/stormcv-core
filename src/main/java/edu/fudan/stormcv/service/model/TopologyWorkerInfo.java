package edu.fudan.stormcv.service.model;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 8:52 PM
 * Description:
 */
public class TopologyWorkerInfo {
    private String topoName;
    private String host;
    private long pid;
    private int port;
    private double cpuUsage;
    private double memoryUsage;

    public TopologyWorkerInfo(String topoName, String host, long pid, int port, double cpuUsage, double memoryUsage) {
        this.topoName = topoName;
        this.host = host;
        this.pid = pid;
        this.port = port;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
    }

    public String getTopoName() {
        return topoName;
    }

    public void setTopoName(String topoName) {
        this.topoName = topoName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    @Override
    public String toString() {
        return "TopologyWorkerInfo{" +
                "topoName='" + topoName + '\'' +
                ", host='" + host + '\'' +
                ", pid=" + pid +
                ", port=" + port +
                ", cpuUsage=" + cpuUsage +
                ", memoryUsage=" + memoryUsage +
                '}';
    }
}
