package edu.fudan.stormcv.testcase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.fudan.stormcv.constant.GlobalConstants;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.model.serializer.ImageRequestSerializer;
import edu.fudan.stormcv.spout.SpoutSignalClient;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/3/17 - 8:19 AM
 * Description:
 */
public class SpoutSignalClientTest {
    private static final Logger logger = LoggerFactory.getLogger(SpoutSignalClientTest.class);

    public static void main(String[] args) throws Exception {
        String zkHost = GlobalConstants.ZKHostLocal;
        String streamId = "720p-3";
        String inputLocation = GlobalConstants.TestImageInputDir;
        String outputLocation = GlobalConstants.TestImageOuputDir;
        int type = 3;

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        Option zkHostOption = Option.builder("z")
                .hasArgs()
                .desc("zkHost")
                .build();
        options.addOption(zkHostOption);
        Option streamOption = Option.builder("s")
                .hasArgs()
                .desc("stream")
                .build();
        options.addOption(streamOption);
        Option inputOption = Option.builder("i")
                .hasArgs()
                .desc("input")
                .build();
        options.addOption(inputOption);
        Option outputOption = Option.builder("o")
                .hasArgs()
                .desc("output")
                .build();
        options.addOption(outputOption);
        Option typeOption = Option.builder("t")
                .hasArgs()
                .desc("type")
                .build();
        options.addOption(typeOption);

        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (commandLine.hasOption("z")) {
            zkHost =  commandLine.getOptionValue("z");
        }
        if (commandLine.hasOption("s")) {
            streamId =  commandLine.getOptionValue("s");
        }
        if (commandLine.hasOption("i")) {
            inputLocation =  commandLine.getOptionValue("i");
        }
        if (commandLine.hasOption("o")) {
            outputLocation =  commandLine.getOptionValue("o");
        }
        if (commandLine.hasOption("t")) {
            type =  Integer.valueOf(commandLine.getOptionValue("t"));
        }

        SpoutSignalClient sc = new SpoutSignalClient(zkHost, GlobalConstants.ImageRequestZKRoot);
        sc.start();
        try {
            ImageRequest request = new ImageRequest(streamId, inputLocation, outputLocation, type);
            sc.sendImageRequest(request);
        } finally {
            sc.close();
        }
    }
}
