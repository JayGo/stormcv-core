package edu.fudan.stormcv.util;

import de.flapdoodle.embed.process.config.ISupportConfig;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.StreamToLineProcessor;
import de.flapdoodle.embed.process.runtime.Processes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/24/17 - 12:58 PM
 * Description:
 */
public class ServerUtil {

    private static final Logger logger = LoggerFactory.getLogger(ServerUtil.class);

    public static String getHostname() {
        String hostName = null;
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            if (inetAddress != null) {
                hostName = inetAddress.getHostName();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostName;
    }

    public static String getIp() {
        String ip = null;
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            if (inetAddress != null) {
                ip = inetAddress.getHostAddress();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return ip;
    }

    public static long runLocalCommand(String command) {
        if (command == null || command.isEmpty()) {
            return -1;
        }
        logger.info("local Command: {}", command);

        Long pid = -1L;
        String[] commandArgs = command.split(" ");
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(commandArgs);
            Process process = processBuilder.start();
            pid = Processes.processId(process);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        return pid;
    }

    public static long runRemoteCommand(String command, String username, String password) {
        return 0;
    }

    public static boolean killLocalProcess(long pid) {
        if (Processes.isProcessRunning(Platform.Linux, pid)) {
            return Processes.killProcess(new ISupportConfig() {
                @Override
                public String getName() {
                    return "generic";
                }

                @Override
                public String getSupportUrl() {
                    return "https://github.com/flapdoodle-oss/de.flapdoodle.embed.process";
                }

                @Override
                public String messageOnException(Class<?> aClass, Exception e) {
                    return null;
                }
            }, Platform.Linux, StreamToLineProcessor.wrap(Processors.console()), pid);
        }
        return true;
    }

    public static boolean killRemoteProcess(long pid) {
        return false;
    }
}
