package edu.fudan.stormcv.service.db;

import edu.fudan.stormcv.service.model.TopologyBasicInfo;
import edu.fudan.stormcv.service.model.TopologyComponentInfo;
import edu.fudan.stormcv.service.model.TopologyWorkerInfo;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 5/1/17 - 5:29 PM
 * Description:
 */
public interface TopologyDao {
    boolean addTopologyBasicInfo(TopologyBasicInfo info);
    boolean deleteTopologyBasicInfo(String topoName);
    TopologyBasicInfo getTopologyBasicInfo(String topoName);

    boolean addTopologyWorkerInfo(List<TopologyWorkerInfo> workerInfoList);
    boolean addTopologyWorkerInfo(TopologyWorkerInfo workerInfo);
    boolean deleteAllTopologyWorkerInfo(String topoName);
    double getTopologyTotalCpuUsage(String topoName);
    double getTopologyTotalMemoryUsage(String topoName);

    boolean addTopologyComponentInfo(List<TopologyComponentInfo> componentInfoList);
    boolean addTopologyComponentInfo(TopologyComponentInfo componentInfo);
    boolean deleteAllTopologyComponentInfo(String topoName);
    List<TopologyComponentInfo> getAllTopologyComponentInfo(String topoName);
}
