package edu.fudan.stormcv.spout;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.model.serializer.ImageRequestSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 3/5/17 - 6:00 AM
 * Description:
 */
public class SpoutSignalClient {
    private static final Logger logger = LoggerFactory.getLogger(SpoutSignalClient.class);

    private CuratorFramework client = null;
    private String name;
    private Kryo kryo;
    private ImageRequestSerializer serializer;

    public SpoutSignalClient(String zkConnectString, String name) {
        if (!name.startsWith("/")) {
            name = "/" + name;
        }
        this.name = name;
        this.client = CuratorFrameworkFactory.builder().namespace("storm-signals").connectString(zkConnectString)
                .retryPolicy(new RetryOneTime(500)).build();
        logger.debug("created Curator client");

        kryo = new Kryo();
        serializer = new ImageRequestSerializer();
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
            logger.info("Created {} on the ZooKeeper", path);
        }
        this.client.setData().forPath(this.name, signal);
    }

    public void sendImageRequest(ImageRequest request) {
        logger.info("send request: " + request);
        Output output = new Output(4096, 409600);
        serializer.write(kryo, output, request);
        byte[] bytes = output.toBytes();
        try {
            this.send(bytes);
        } catch (Exception e) {
            logger.error("Unable to send image process request due to {}", e.getMessage());
        }
        logger.info("send the signal success!");
    }
}
