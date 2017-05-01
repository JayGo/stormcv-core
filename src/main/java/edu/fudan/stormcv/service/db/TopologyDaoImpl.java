package edu.fudan.stormcv.service.db;

import edu.fudan.stormcv.service.model.TopologyBasicInfo;
import edu.fudan.stormcv.service.model.TopologyComponentInfo;
import edu.fudan.stormcv.service.model.TopologyWorkerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 5/1/17 - 6:05 PM
 * Description:
 */
public class TopologyDaoImpl implements TopologyDao {

    private static final Logger logger = LoggerFactory.getLogger(TopologyDaoImpl.class);
    private DBManager dbManager = DBManager.getInstance();

    @Override
    public boolean addTopologyBasicInfo(TopologyBasicInfo info) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("INSERT INTO " + DBManager.TOPOLOGY_TABLE + " "
                    + DBManager.TOPOLOGY_TABLE_COLS + " VALUES(?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE topo_id=?, worker_num=?, owner=?, uptime_secs=?, task_num=?, " +
                            "executor_num=?, status=?", new Object[] {info.getTopoName(), info.getTopoId(),
                    info.getWorkerNum(), info.getOwner(), info.getUptimeSecs(), info.getTaskNum(),
                    info.getExecutorNum(), info.getStatus().getCode(), info.getTopoId(), info.getWorkerNum(),
                    info.getOwner(), info.getUptimeSecs(), info.getTaskNum(), info.getExecutorNum(),
                    info.getStatus().getCode()});
        } catch (Exception e) {
            ret = false;
            logger.info("add topology {} failed due to {}", info.getTopoName(), e.getMessage());
        }
        if (ret) {
            logger.info("add topology success, topoName={}", info.getTopoName());
        }
        return ret;
    }

    @Override
    public boolean deleteTopologyBasicInfo(String topoName) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("DELETE FROM " + DBManager.TOPOLOGY_TABLE + " WHERE topo_name = ?",
                    new Object[] { topoName });
        } catch (Exception e) {
            ret = false;
            logger.info("delete topology {} failed due to {}", topoName, e.getMessage());
        }
        if (ret) {
            logger.info("delete topology {} success", topoName);
        }
        return ret;
    }

    @Override
    public TopologyBasicInfo getTopologyBasicInfo(String topoName) {
        TopologyBasicInfo ret = null;
        try {
            Map<String, Object> queryRet = dbManager.getJdbcTemplate().queryForMap(
                    "SELECT * FROM " + DBManager.TOPOLOGY_TABLE + " WHERE topo_name = ?", new Object[] { topoName });
            ret = new TopologyBasicInfo();
            ret.setTopoName(topoName);
            ret.setTopoId((String) queryRet.get("topo_id"));
            ret.setOwner((String)queryRet.get("owner"));
            ret.setWorkerNum((int)queryRet.get("workerNum"));
            ret.setUptimeSecs((int)queryRet.get("uptime_secs"));
            ret.setTaskNum((int)queryRet.get("task_num"));
            ret.setExecutorNum((int)queryRet.get("executor_num"));
            logger.info(ret.toString());
        } catch (Exception e) {
            logger.info("cannot select the topology info for {}", topoName);
        }
        return ret;
    }

    @Override
    public boolean addTopologyWorkerInfo(List<TopologyWorkerInfo> workerInfoList) {
        boolean ret = true;
        for (TopologyWorkerInfo info : workerInfoList) {
            if (!addTopologyWorkerInfo(info)) {
                ret = false;
            }
        }
        return ret;
    }

    @Override
    public boolean addTopologyWorkerInfo(TopologyWorkerInfo info) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("INSERT INTO " + DBManager.TOPOLOGY_WORKER_INFO_TABLE + " "
                            + DBManager.TOPOLOGY_WORKER_INFO_TABLE_COLS + " VALUES(?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE pid=?, cpu_usage=?, memory_usage=?",
                    new Object[] {info.getTopoName(), info.getHost(), info.getPid(), info.getPort(),
                            info.getCpuUsage(), info.getMemoryUsage(), info.getPid(),
                            info.getCpuUsage(), info.getMemoryUsage()});
        } catch (Exception e) {
            ret = false;
            logger.info("add topology {} worker info {} failed due to {}", info.getTopoName(),
                    info.toString(), e.getMessage());
        }
        if (ret) {
            logger.info("add topology worker info success, info: {}", info.toString());
        }
        return ret;
    }

    @Override
    public boolean deleteAllTopologyWorkerInfo(String topoName) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("DELETE FROM " + DBManager.TOPOLOGY_WORKER_INFO_TABLE +
                            " WHERE topo_name = ?", new Object[] { topoName });
        } catch (Exception e) {
            ret = false;
            logger.info("delete topology {} worker info failed due to {}", topoName, e.getMessage());
        }
        if (ret) {
            logger.info("delete topology {} worker info success", topoName);
        }
        return ret;
    }

    @Override
    public double getTopologyTotalCpuUsage(String topoName) {
        double totalCpuUsage = 0.0;
        try {
            List<Double> ret = dbManager.getJdbcTemplate().queryForList("SELECT cpu_usage FROM " + DBManager.TOPOLOGY_WORKER_INFO_TABLE
                    + " WHERE topo_name = ?", new Object[] { topoName }, Double.class);
            for (Double usage : ret) {
                totalCpuUsage += usage;
            }
        } catch (Exception e) {
            logger.error("cannot get topology {} cpu usage due to {}", topoName, e.getMessage());
        }
        return totalCpuUsage;
    }

    @Override
    public double getTopologyTotalMemoryUsage(String topoName) {
        double totalMemoryUsage = 0.0;
        try {
            List<Double> ret = dbManager.getJdbcTemplate().queryForList("SELECT memory_usage FROM " + DBManager.TOPOLOGY_WORKER_INFO_TABLE
                    + " WHERE topo_name = ?", new Object[] { topoName }, Double.class);
            for (Double usage : ret) {
                totalMemoryUsage += usage;
            }
        } catch (Exception e) {
            logger.error("cannot get topology {} cpu usage due to {}", topoName, e.getMessage());
        }
        return totalMemoryUsage;
    }

    @Override
    public boolean addTopologyComponentInfo(List<TopologyComponentInfo> componentInfoList) {
        boolean ret = true;
        for (TopologyComponentInfo info : componentInfoList) {
            if (!addTopologyComponentInfo(info)) {
                ret = false;
            }
        }
        return ret;
    }

    @Override
    public boolean addTopologyComponentInfo(TopologyComponentInfo info) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("INSERT INTO " + DBManager.TOPOLOGY_COMPONENT_INFO_TABLE + " "
                            + DBManager.TOPOLOGY_COMPONENT_INFO_COLS + " VALUES(?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE type=?, executor_num=?, task_num=?, alltime_processed=?, " +
                            "alltime_failed=?, alltime_latency=?",
                    new Object[] {info.getTopoName(), info.getComponentId(), info.getType(), info.getExecutorNum(),
                            info.getTaskNum(), info.getAllTimeProcessed(), info.getAllTimeFailed(),
                            info.getAllTimeLatency(), info.getType(), info.getExecutorNum(), info.getTaskNum(),
                            info.getAllTimeProcessed(), info.getAllTimeFailed(), info.getAllTimeLatency()});
        } catch (Exception e) {
            ret = false;
            logger.info("add topology {} component {} info {} failed due to {}", info.getTopoName(),
                    info.getComponentId(), info.toString(), e.getMessage());
        }
        if (ret) {
            logger.info("add topology component info success, info: {}", info.toString());
        }
        return ret;
    }

    @Override
    public boolean deleteAllTopologyComponentInfo(String topoName) {
        boolean ret = true;
        try {
            dbManager.getJdbcTemplate().update("DELETE FROM " + DBManager.TOPOLOGY_COMPONENT_INFO_TABLE +
                    " WHERE topo_name = ?", new Object[] { topoName });
        } catch (Exception e) {
            ret = false;
            logger.info("delete topology {} component info failed due to {}", topoName, e.getMessage());
        }
        if (ret) {
            logger.info("delete topology {} component info success", topoName);
        }
        return ret;
    }

    @Override
    public List<TopologyComponentInfo> getAllTopologyComponentInfo(String topoName) {
        List<TopologyComponentInfo> componentInfoList = new ArrayList<>();
        try {
            Map<String, Object> queryRet = dbManager.getJdbcTemplate().queryForMap(
                    "SELECT * FROM " + DBManager.TOPOLOGY_COMPONENT_INFO_TABLE + " WHERE topo_name = ?", new Object[] { topoName });
            TopologyComponentInfo info = new TopologyComponentInfo();
            info.setTopoName(topoName);
            info.setComponentId((String)queryRet.get("component_id"));
            info.setType((String)queryRet.get("type"));
            info.setExecutorNum((int)queryRet.get("executor_num"));
            info.setTaskNum((int)queryRet.get("task_num"));
            info.setAllTimeProcessed((long)queryRet.get("alltime_processed"));
            info.setAllTimeFailed((long)queryRet.get("alltime_failed"));
            info.setAllTimeLatency((double)queryRet.get("alltime_latency"));
            componentInfoList.add(info);
        } catch (Exception e) {
            logger.info("cannot select the topology info for {}", topoName);
        }
        return componentInfoList;
    }
}
