package edu.fudan.stormcv.service.process;

import edu.fudan.stormcv.service.db.TopologyDao;
import edu.fudan.stormcv.service.db.TopologyDaoImpl;
import edu.fudan.stormcv.topology.TopologyH264;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/28/17 - 9:36 AM
 * Description:
 */
public class TopologyManager {

    public static TopologyManager instance = null;
    public static final Logger logger = LoggerFactory.getLogger(TopologyManager.class);

    private Map<String, TopologyH264> topologyH264Map;

    private TopologyDao topologyDao;

    private TopologyManager() {
        this.topologyH264Map = new HashMap<>();
        this.topologyDao = new TopologyDaoImpl();
    }

    public static synchronized TopologyManager getInstance() {
        if (instance == null) {
            instance = new TopologyManager();
        }
        return instance;
    }

    public Map<String, TopologyH264> getTopologyH264Map() {
        return this.topologyH264Map;
    }

    public void addTopology(String topoName, TopologyH264 topologyH264) {
        this.topologyH264Map.put(topoName, topologyH264);
    }

    public String killTopology(String topoName) {
        TopologyH264 topologyH264 = topologyH264Map.get(topoName);
        String retStr = null;
        if (topologyH264 != null) {
            this.topologyDao.deleteTopologyBasicInfo(topoName);
            this.topologyDao.deleteAllTopologyComponentInfo(topoName);
            this.topologyDao.deleteAllTopologyWorkerInfo(topoName);
            retStr = topologyH264.killTopology();
        } else {
            retStr = new String("Error: NO alive topology named " + topoName);
        }
        return retStr;
    }

}
