package edu.fudan.stormcv.constant;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 8:43 PM
 * Description:
 */
public enum TopologyStatus {
    ACTIVE(0, "ACTIVE"),
    INACTIVE(1, "INACTIVE"),
    KILLED(2, "KILLED"),
    REBALANCING(3, "REBALANCING");

    private String status = "";
    private int code;

    TopologyStatus(int code, String status) {
        this.code = code;
        this.status = status;
    }

    public int getCode() {
        return this.code;
    }

    public String toString() {
        return this.status;
    }
}
