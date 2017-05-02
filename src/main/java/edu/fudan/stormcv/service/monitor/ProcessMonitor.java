package edu.fudan.stormcv.service.monitor;

import edu.fudan.stormcv.service.db.DBManager;
import edu.fudan.stormcv.service.process.ProcessManager;
import edu.fudan.stormcv.service.process.TopologyManager;
import edu.fudan.stormcv.topology.TopologyH264;

import java.util.Map;

import static java.lang.Thread.sleep;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 9:53 AM
 * Description:
 */
public class ProcessMonitor implements Runnable {

    private MetricsCollector metricsCollector;

    public ProcessMonitor() {
        metricsCollector = new MetricsCollector();
    }

    @Override
    public void run() {
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        checkProcess();
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        checkTopology();
    }

    private void checkProcess() {

    }

    private void checkTopology() {
        Map<String, TopologyH264>  topologyH264Map = TopologyManager.getInstance().getTopologyH264Map();

        for (String topoName : topologyH264Map.keySet()) {
            TopologyH264 topology = topologyH264Map.get(topoName);
            metricsCollector.setName(topoName, topology.getConfig());
            metricsCollector.run();
//            if (topology.isTopologyRunningAtLocal()) {
//
//            } else {
//
//            }
        }
    }

    private boolean updateTopologySql() {
        return true;
    }
}
