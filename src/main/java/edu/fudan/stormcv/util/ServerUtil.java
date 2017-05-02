package edu.fudan.stormcv.util;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import de.flapdoodle.embed.process.config.ISupportConfig;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.StreamToLineProcessor;
import de.flapdoodle.embed.process.runtime.Processes;
import edu.fudan.stormcv.constant.ServerConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/24/17 - 12:58 PM
 * Description:
 */
public class ServerUtil {

    public static String PROMPT = "GO1GO2GO3";
    final private static char LF = '\n';
    final private static char CR = '\r';

    public static void main(String[] args) {
        long pid = 23331;
        logger.info("cpu:{}", ServerUtil.getLocalProcessCpuUsage(pid));
        logger.info("memmory:{} MB", ServerUtil.getLocalProcessMemoryUsage(pid));
        logger.info("pid for port {}: {}", 80, ServerUtil.getLocalPidForPort(80));

        try {
            pid = ServerUtil.runRemoteCommand("top", "10.134.142.141", "root", "lab369141");
            logger.info("start remote process {} for command top", pid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String host = "10.134.142.141";
        String command = "kill -9 " + pid;
        String retStr = null;
        try {
            ServerConstant.UserInfo userInfo = ServerConstant.ServerAuthentication.get(host);
            retStr = ServerUtil.runRemoteOnceCommand(command, host, userInfo.getUsername(), userInfo.getPassword());
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("run {} command on host chick, output {}", command, retStr);

        host = "10.134.142.114";
        pid = 23331;
        ServerConstant.UserInfo userInfo = ServerConstant.ServerAuthentication.get(host);
        double cpuUsage = ServerUtil.getRemoteProcessCpuUsage(host, pid, userInfo.getUsername(), userInfo.getPassword());
        double memUsage = ServerUtil.getRemoteProcessMemoryUsage(host, pid, userInfo.getUsername(), userInfo.getPassword());
        logger.info("host {} and pid {}, cpu:{}, memmory: {}", host, pid, cpuUsage, memUsage);

        logger.info("host {}, pid for port {}: {}", host, 80, ServerUtil.getRemotePidForPort(host, 80, userInfo.getUsername(), userInfo.getPassword()));
    }

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
            e1.printStackTrace();
        }

        return pid;
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

    public static double getLocalProcessCpuUsage(long pid) {
        Process process = null;
        BufferedReader br = null;
        String command = "top -b -n 1 -p " + pid;
//        String command = "top -b -n 1 -p " + pid + " | sed 'd' | sed -n '$p' | awk '{print $9}'";
        Double cpu = 0.0;
        try {
            String[] commands = command.split(" ");
            process = Runtime.getRuntime().exec(commands);
            int resultCode = process.waitFor();
            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("" + pid)) {
                    String[] records = line.split(" +");
                    cpu = Double.parseDouble(records[8]);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query cpuinfo for pid {} failed", pid);
        } finally {
            try {
                if (process != null) process.destroy();
                if (br != null) br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cpu;
    }

    public static double getLocalProcessMemoryUsage(long pid) {
        Process process = null;
        BufferedReader br = null;
//        String command = "cat /proc/" + pid + "/status | grep VmRSS | awk \'{print $2}\'";
        String command = "cat /proc/" + pid + "/status";
        Double mem = 0.0;
        try {
            process = Runtime.getRuntime().exec(command);
            int resultCode = process.waitFor();
            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    String[] records = line.split(" +");
                    long memKB = Long.parseLong(records[1]);
                    mem = memKB / 1024.0; // to MB
                    mem = (long)(mem * 100) / 100.0;
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query memoryInfo for pid {} failed", pid);
        } finally {
            try {
                if (process != null) process.destroy();
                if (br != null) br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return mem;
    }

    public static long getLocalPidForPort(int port) {
        Process process = null;
        BufferedReader br = null;
        String command = "lsof -i :" + port;
        long pid = 0L;
        try {
            process = Runtime.getRuntime().exec(command);
            int resultCode = process.waitFor();
            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
            	logger.info("get pid ret line: {}", line);
                if (line.startsWith("java") && line.contains("LISTEN")) {
                    String[] records = line.split(" +");
                    pid = Long.parseLong(records[1]);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query pid for port {} failed", port);
        } finally {
            try {
                if (process != null) process.destroy();
                if (br != null) br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return pid;
    }

    public static long runRemoteCommand(String command, String host, String username, String password) throws IOException {
        Connection c = new Connection(host, 22);
        Session s = null;
        long pid = 0L;
        try {
            c.connect();
            if (!c.authenticateWithPassword(username, password)) {
                throw new IOException("Authentification failed");
            }
            s = c.openSession();
            s.requestDumbPTY();
            s.startShell();
            String realCommand = command;
            if (!realCommand.endsWith("&")) {
                realCommand += " & ";
            }
            realCommand = "setsid " + realCommand + "ps axf | grep '\\<top\\>' | grep -v grep";
            logger.info("run remote command on {}: {}", realCommand, host);
            InputStream is = s.getStdout();
            OutputStream os = s.getStdin();
            InputStream es = s.getStderr();
            final PushbackInputStream pbis = new PushbackInputStream(new StreamGobbler(is));

            writeCmd(os, pbis, "PS1=" + PROMPT);
            readTillPrompt(pbis, null);

            final List<String> lines = new LinkedList<>();
            writeCmd(os, pbis, realCommand);
            readTillPrompt(pbis, lines);

            for (String line : lines) {
                if (line.contains(command) && !line.contains("setsid") && line.contains("0:0")) {
                    logger.debug("line: {}", line);
                    String[] records = line.split(" +");
                    pid = Long.parseLong(records[0]);
                    break;
                }
            }
        } finally {
            if (s != null) {
                s.close();
            }
            c.close();
        }
        return pid;
    }

    public static String runRemoteOnceCommand(String command, String host, String username, String password) throws IOException {
        StringBuilder builder = new StringBuilder();
        try {
            /* Create a connection instance */
            Connection conn = new Connection(host, 22);
            /* Now connect */
            conn.connect();
            /* Authenticate.
             * If you get an IOException saying something like
             * "Authentication method password not supported by the server at this stage."
             * then please check the FAQ.
             */
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (isAuthenticated == false)
                throw new IOException("Authentication failed.");
            /* Create a session */
            Session sess = conn.openSession();
            sess.execCommand(command);
            /*
             * This basic example does not handle stderr, which is sometimes dangerous
             * (please read the FAQ).
             */
            InputStream stdout = new StreamGobbler(sess.getStdout());
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                builder.append(line).append("\n");
            }
            /* Close this session */
            sess.close();
            /* Close the connection */
            conn.close();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            System.exit(2);
        }

        return builder.toString();
    }


    public static boolean killRemoteProcess(String host, long pid, String username, String password) {
        String command = "kill -9 " + pid;
        String retStr = null;
        try {
            retStr = runRemoteOnceCommand(command, host, username, password);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (retStr == null || retStr.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    public static double getRemoteProcessCpuUsage(String host, long pid, String username, String password) {
        String command = "top -b -n 1 -p " + pid;
        Double cpu = 0.0;
        try {
            String[] lines = runRemoteOnceCommand(command, host, username, password).split("\n");
            for (String line : lines) {
                if (line.startsWith("" + pid)) {
                    String[] records = line.split(" +");
                    cpu = Double.parseDouble(records[8]);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query cpuinfo for host {} and pid {} failed", host, pid);
        }
        return cpu;
    }

    public static double getRemoteProcessMemoryUsage(String host, long pid, String username, String password) {
        String command = "cat /proc/" + pid + "/status";
        Double mem = 0.0;
        try {
            String[] lines = runRemoteOnceCommand(command, host, username, password).split("\n");
            for (String line : lines) {
                if (line.startsWith("VmRSS:")) {
                    String[] records = line.split(" +");
                    long memKB = Long.parseLong(records[1]);
                    mem = memKB / 1024.0; // to MB
                    mem = (long)(mem * 100) / 100.0;
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query memoryInfo for pid {} failed", pid);
        }

        return mem;
    }

    public static long getRemotePidForPort(String host, int port, String username, String password) {
        String command = "lsof -i :" + port;
        long pid = 0L;
        try {
            String[] lines = runRemoteOnceCommand(command, host, username, password).split("\n");
            for (String line : lines) {
                if (line.startsWith("java") && line.contains("LISTEN")) {
                    String[] records = line.split(" +");
                    pid = Long.parseLong(records[1]);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("query pid for port {} failed", port);
        }
        return pid;
    }

    private static void writeCmd(final OutputStream os,
                                 final PushbackInputStream is,
                                final String cmd) throws IOException {
        os.write(cmd.getBytes());
        os.write(LF);
        skipTillEndOfCommand(is);
    }

    private static void readTillPrompt(final InputStream is,
                                      final Collection<String> lines) throws IOException {

        final StringBuilder cl = new StringBuilder();
        boolean eol = true;
        int match = 0;
        while (true) {
            final char ch = (char) is.read();
            switch (ch) {
                case CR:
                case LF:
                    if (!eol) {
                        if (lines != null) {
                            lines.add(cl.toString());
                        }
                        cl.setLength(0);
                    }
                    eol = true;
                    break;
                default:
                    if (eol) {
                        eol = false;
                    }
                    cl.append(ch);
                    break;
            }

            if (cl.length() > 0
                    && match < PROMPT.length()
                    && cl.charAt(match) == PROMPT.charAt(match)) {
                match++;
                if (match == PROMPT.length()) {
                    return;
                }
            } else {
                match = 0;
            }
        }
    }

    private static void skipTillEndOfCommand(final PushbackInputStream is) throws IOException {
        boolean eol = false;
        while (true) {
            final char ch = (char) is.read();
            switch (ch) {
                case CR:
                case LF:
                    eol = true;
                    break;
                default:
                    if (eol) {
                        is.unread(ch);
                        return;
                    }
            }
        }
    }
}
