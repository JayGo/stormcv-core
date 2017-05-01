package edu.fudan.stormcv.service.monitor;

import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.constant.ServerConstant;
import edu.fudan.stormcv.service.db.TopologyDao;
import edu.fudan.stormcv.service.db.TopologyDaoImpl;
import edu.fudan.stormcv.service.model.TopologyBasicInfo;
import edu.fudan.stormcv.service.model.TopologyComponentInfo;
import edu.fudan.stormcv.service.model.TopologyWorkerInfo;
import edu.fudan.stormcv.topology.TopologyH264;
import edu.fudan.stormcv.util.LibLoader;
import edu.fudan.stormcv.util.ServerUtil;
import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/29/17 - 10:41 AM
 * Description:
 */
public class MetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    public static void main(String[] args) {
        LibLoader.loadOpenCVLib();
        LibLoader.loadHgCodecLib();

        String topoName = "jkyan-test";
        TopologyH264 topologyH264 = new TopologyH264(topoName, GlobalConstants.DefaultRTMPServer, GlobalConstants.Pseudo720pRtspAddress, "gray").isTopologyRunningAtLocal(false);
        Config config = topologyH264.getConfig();
        try {
            topologyH264.submitTopology();
        } catch (Exception e) {
            e.printStackTrace();
        }

        MetricsCollector collector = new MetricsCollector();
        collector.setName(topoName, config);
        while (true) {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.run();

            logger.info("\n\n");
            logger.info("===============================start=================================================");

            logger.info("basic info for topology {}: {}", topoName, collector.getBasicInfo().toString());
            List<TopologyComponentInfo> componentInfoList = collector.getComponentInfoList();
            for (TopologyComponentInfo info : componentInfoList) {
                logger.info("component info: {}", info.toString());
            }

            List<TopologyWorkerInfo> workerInfoList = collector.getWorkerInfoList();
            for (TopologyWorkerInfo info : workerInfoList) {
                logger.info("worker info: {}", info.toString());
            }
            logger.info("===============================end=================================================");
        }
    }

    private static final String ALL_TIME = ":all-time";
    private String name;
    private Nimbus.Client client;
    private TopologyBasicInfo basicInfo = null;
    private List<TopologyComponentInfo> componentInfoList = null;
    private List<TopologyWorkerInfo> workerInfoList = null;
    private Set<WorkerSlot> workerSlotSet;
    private TopologyDao topologyDao;

    public MetricsCollector() {
        this.componentInfoList = new ArrayList<>();
        this.workerInfoList = new ArrayList<>();
        this.workerSlotSet = new HashSet<>();
        topologyDao = new TopologyDaoImpl();
    }

    public void setName(String name, Config config) {
        this.name = name;
        this.client = NimbusClient.getConfiguredClient(config).getClient();
    }

    public TopologyBasicInfo getBasicInfo() {
        return basicInfo;
    }

    public List<TopologyComponentInfo> getComponentInfoList() {
        return componentInfoList;
    }

    public List<TopologyWorkerInfo> getWorkerInfoList() {
        return workerInfoList;
    }

    public Set<WorkerSlot> getWorkerSlotSet() {
        return workerSlotSet;
    }

    public void run() {
        pollTopologyComponentInfo();
        pollTopologyWorkerInfo();
        if (this.basicInfo != null) {
            topologyDao.addTopologyBasicInfo(this.basicInfo);
        }
        if (this.componentInfoList != null && !this.componentInfoList.isEmpty()) {
            topologyDao.addTopologyComponentInfo(this.componentInfoList);
        }
        if (this.workerInfoList != null && !(this.workerInfoList.isEmpty())) {
            topologyDao.addTopologyWorkerInfo(this.workerInfoList);
        }
    }

    public boolean pollTopologyComponentInfo() {
        this.basicInfo = null;
        this.componentInfoList.clear();
        this.workerSlotSet.clear();

        ClusterSummary clusterSummary = null;
        try {
            clusterSummary = this.client.getClusterInfo();
        } catch (TException e) {
            e.printStackTrace();
        }

        if (clusterSummary == null) {
            logger.error("[MetricsCollector]cannot get cluster summary");
            return false;
        }

        TopologySummary topologySummary = null;
        for (TopologySummary summary : clusterSummary.get_topologies()) {
            if (summary.get_name().equals(this.name)) {
                topologySummary = summary;
                break;
            }
        }

        if (topologySummary == null) {
            logger.error("[MetricsCollector]cannot find TopologySummary for topology {}", this.name);
            return false;
        }

        String topoId = topologySummary.get_id();
        this.basicInfo = new TopologyBasicInfo(this.name, topoId, topologySummary.get_num_workers(), topologySummary.get_owner(),
                topologySummary.get_uptime_secs(), topologySummary.get_num_tasks(), topologySummary.get_num_executors(), topologySummary.get_status());

        TopologyPageInfo pageInfo = null;
        try {
            pageInfo = this.client.getTopologyPageInfo(topoId, ALL_TIME, false);
        } catch (TException e) {
            e.printStackTrace();
        }

        if (pageInfo != null) {
            Map<String, ComponentAggregateStats> spoutAggregateStatsMap = pageInfo.get_id_to_spout_agg_stats();
            for (String spoutId : spoutAggregateStatsMap.keySet()) {
                ComponentAggregateStats componentAggregateStats = spoutAggregateStatsMap.get(spoutId);
                CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
                int executorNum = commonStats.get_num_executors();
                int taskNum = commonStats.get_num_tasks();
                long emitted = commonStats.get_emitted();
                long failed = commonStats.get_failed();
                double completeLatency = componentAggregateStats.get_specific_stats().get_spout().get_complete_latency_ms();
                completeLatency = (long)(completeLatency * 100) / 100.0;
                TopologyComponentInfo info = new TopologyComponentInfo(this.name, spoutId, "spout", executorNum, taskNum, emitted, failed, completeLatency);
                this.componentInfoList.add(info);
            }

            Map<String, ComponentAggregateStats> boltAggregateStatsMap = pageInfo.get_id_to_bolt_agg_stats();
            for (String boltId : boltAggregateStatsMap.keySet()) {
                ComponentAggregateStats componentAggregateStats = boltAggregateStatsMap.get(boltId);
                CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
                BoltAggregateStats boltStats = componentAggregateStats.get_specific_stats().get_bolt();
                int executorNum = commonStats.get_num_executors();
                int taskNum = commonStats.get_num_tasks();
                long executed = boltStats.get_executed();
                long failed = commonStats.get_failed();
                double executedLatency = boltStats.get_execute_latency_ms();
                executedLatency = (long)(executedLatency * 100) / 100.0;
                TopologyComponentInfo info = new TopologyComponentInfo(this.name, boltId, "bolt", executorNum, taskNum, executed, failed, executedLatency);
                this.componentInfoList.add(info);
            }
        } else {
            logger.error("[MetricsCollector]cannot find Topology PageInfo for topology {}", topoId);
            return false;
        }

        TopologyInfo topologyInfo = null;
        try {
            topologyInfo = this.client.getTopologyInfo(topoId);
        } catch (TException e) {
            e.printStackTrace();
        }

        if (topologyInfo == null) {
            logger.error("[MetricsCollector]cannot find Topology for topology {}", topoId);
            return false;
        }

        for (ExecutorSummary es : topologyInfo.get_executors()) {
            if (Utils.isSystemId(es.get_component_id())) {
                continue;
            }
            String host = es.get_host();
            int port = es.get_port();
            this.workerSlotSet.add(new WorkerSlot(host, port));
        }

        return true;
    }

    public boolean pollTopologyWorkerInfo() {
        this.workerInfoList.clear();
        if (this.workerSlotSet == null || this.workerSlotSet.isEmpty()) {
            logger.error("no running worker for topology {}", this.name);
            return false;
        }
        for (WorkerSlot workerSlot : this.workerSlotSet) {
            String host = workerSlot.getNodeId();
            int port = workerSlot.getPort();

            if (host.equals(ServerUtil.getHostname())) {
                long pid = ServerUtil.getLocalPidForPort(port);
                double cpuUsage = ServerUtil.getLocalProcessCpuUsage(pid);
                double memoryUsage = ServerUtil.getLocalProcessMemoryUsage(pid);
                TopologyWorkerInfo info = new TopologyWorkerInfo(this.name, host, pid, port, cpuUsage, memoryUsage);
                this.workerInfoList.add(info);
            } else {
                ServerConstant.UserInfo userInfo = ServerConstant.ServerAuthentication.get(host);
                long pid = ServerUtil.getRemotePidForPort(host, port, userInfo.getUsername(), userInfo.getPassword());
                double cpuUsage = ServerUtil.getRemoteProcessCpuUsage(host, pid, userInfo.getUsername(), userInfo.getPassword());
                double memoryUsage = ServerUtil.getRemoteProcessMemoryUsage(host, pid, userInfo.getUsername(), userInfo.getPassword());
                TopologyWorkerInfo info = new TopologyWorkerInfo(this.name, host, pid, port, cpuUsage, memoryUsage);
                this.workerInfoList.add(info);
            }
        }

        return true;
    }
}
