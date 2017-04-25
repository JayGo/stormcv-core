package edu.fudan.stormcv.service.process;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/24/17 - 3:34 PM
 * Description:
 */
public class WorkerInfo {
    private String hostIp;
    private long pid;

    public WorkerInfo(String hostIp, long pid) {
        this.hostIp = hostIp;
        this.pid = pid;
    }

    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }
}
