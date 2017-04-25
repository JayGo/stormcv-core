package edu.fudan.stormcv.service.process;

import de.flapdoodle.embed.process.config.ISupportConfig;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.StreamToLineProcessor;
import de.flapdoodle.embed.process.runtime.Processes;
import edu.fudan.stormcv.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/24/17 - 2:17 PM
 * Description:
 */
public class ProcessManager {

    private static final Logger logger = LoggerFactory.getLogger(ProcessManager.class);
    private static ProcessManager instance = null;

    private Map<String, List<Long>> serverPidMap;
    private Map<Long, String> processCommandMap;
    private Map<Long, Process> pidProcessMap;

    private static final String[] AVAIABLE_FFMPEG_SERVERS = {"10.134.142.114"};

    private static final String localAddress = ServerUtil.getIp();

    private Random random;

    public static void main(String[] args) {
        logger.info("hostname:{}", ServerUtil.getHostname());
        logger.info("ip:{}", ServerUtil.getIp());

    }

    private ProcessManager() {
        this.random = new Random();
        this.serverPidMap = new HashMap<>();
        this.processCommandMap = new HashMap<>();
    }

    public static synchronized ProcessManager getInstance() {
        if (null == instance) {
            instance = new ProcessManager();
        }
        return instance;
    }

    public WorkerInfo startProcess(String command) {
        logger.info("start process: {}", command);
        String randomServer = AVAIABLE_FFMPEG_SERVERS[random.nextInt(AVAIABLE_FFMPEG_SERVERS.length)];
        logger.info("randomServer:{}, localServer:{}", randomServer, localAddress);
        long pid = -1L;
        if (randomServer.equals(localAddress)) {
            pid = ServerUtil.runLocalCommand(command);
        } else {
            pid = ServerUtil.runRemoteCommand(command, "jkyan", "jkyan");
        }
        if (pid > 0) {
            if (serverPidMap.get(randomServer) == null) {
                serverPidMap.put(randomServer, new ArrayList<Long>());
            }
            serverPidMap.get(randomServer).add(pid);
        }
        processCommandMap.put(pid, command);
        return new WorkerInfo(randomServer, pid);
    }

    public boolean killProcess(String hostIp, long pid) {
        boolean ret;
        if (hostIp.equals(localAddress)) {
            ret = ServerUtil.killLocalProcess(pid);
        } else {
            ret = ServerUtil.killRemoteProcess(pid);
        }

        List<Long> pids = serverPidMap.get(hostIp);
        if (pids.contains(pid)) {
            int index = pids.indexOf(pid);
            pids.remove(index);
        }
        processCommandMap.remove(pid);
        return ret;
    }
}
