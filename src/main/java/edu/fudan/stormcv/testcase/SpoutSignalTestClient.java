package edu.fudan.stormcv.testcase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.model.serializer.ImageRequestSerializer;
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
public class SpoutSignalTestClient {
    private static final Logger LOG = LoggerFactory.getLogger(SpoutSignalTestClient.class);

    private CuratorFramework client = null;
    private String name;

    public SpoutSignalTestClient(String zkConnectString, String name) {
        this.name = name;
        this.client = CuratorFrameworkFactory.builder().namespace("storm-signals").connectString(zkConnectString)
                .retryPolicy(new RetryOneTime(500)).build();
        LOG.debug("created Curator client");
    }

    public void start() {
        this.client.start();
    }

    public void close() {
        this.client.close();
    }

    public void send(byte[] signal) throws Exception {
        Stat stat = this.client.checkExists().forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
        this.client.setData().forPath(this.name, signal);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String zkHost = "localhost:2000";
        String streamId = "720p";
        String inputLocation = "file:///home/nfs/images/720p/";
        String outputLocation = "file:///home/nfs/images/720p/output/";
        int type = 1;

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

        SpoutSignalTestClient sc = new SpoutSignalTestClient(zkHost, "/request");
        sc.start();
        try {
            System.out.println("start send the image process request");
            ImageRequestSerializer serializer = new ImageRequestSerializer();
            Kryo kryo = new Kryo();
            ImageRequest request = new ImageRequest(streamId, inputLocation, outputLocation, type);
            System.out.println("send request: " + request);
            Output output = new Output(4096, 409600);
            serializer.write(kryo, output, request);
            byte[] bytes = output.toBytes();
            sc.send(bytes);
            System.out.println("send the signal success!");
        } finally {
            sc.close();
        }
    }
}
