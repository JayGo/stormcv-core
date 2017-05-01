package edu.fudan.stormcv.fetcher;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import edu.fudan.stormcv.constant.BoltOperationType;
import edu.fudan.stormcv.model.ImageHandle;
import edu.fudan.stormcv.model.ImageRequest;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.ImageHandleSerializer;
import edu.fudan.stormcv.model.serializer.ImageRequestSerializer;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImageUrlFetcher implements IFetcher<ImageHandle> {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private int batchSize = 64;
    private List<String> locations = null;
    private ConnectorHolder connectorHolder;
    private ImageHandleSerializer serializer = new ImageHandleSerializer();
    private ImageRequestSerializer imageRequestSerializer = new ImageRequestSerializer();
    private int totalTaskSize;
    private int nrTasks;
    private int taskIndex;
    private String streamId = null;
    private String outputLocation = null;
    private Kryo kryo = null;
    private int count = 0;
    private BoltOperationType type;

    public ImageUrlFetcher() {
        this.locations = new ArrayList<>();
    }

    public ImageUrlFetcher(String streamId, String location, String outputLocation, BoltOperationType type) {
        this.locations = new ArrayList<>();
        locations.add(location);
        this.streamId = streamId;
        this.outputLocation = outputLocation;
        this.type = type;
    }

    public ImageUrlFetcher(String streamId, List<String> locations, String outputLocation, BoltOperationType type) {
        this.streamId = streamId;
        this.locations = locations;
        this.outputLocation = outputLocation;
        this.type = type;
    }

    public ImageUrlFetcher batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context)
            throws Exception {
        this.connectorHolder = new ConnectorHolder(stormConf);
        this.kryo = new Kryo();
        nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
        taskIndex = context.getThisTaskIndex();
        partionTasks();
    }

    public void partionTasks() {
        if (locations.size() > 0) {
            List<String> original = new ArrayList<>();
            original.addAll(locations);
            locations.clear();
            for (String dir : original) {
                locations.addAll(expand(dir));
            }

            totalTaskSize = locations.size();

            List<String> filesToFetch = new ArrayList<>();

            int i = taskIndex;
            while (i < locations.size()) {
                filesToFetch.add(locations.get(i));
                i += nrTasks;
            }
            this.locations = filesToFetch;
        }
    }

    @Override
    public CVParticleSerializer<ImageHandle> getSerializer() {
        return serializer;
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
        locations.clear();
    }

    @Override
    public ImageHandle fetchData() {
        ImageHandle imageHandle = null;
        if (locations.size() > 0) {
            List<String> urls = new ArrayList<>();
            while (locations.size() > 0) {
                urls.add(locations.remove(0));
                if (urls.size() >= this.batchSize) {
                    break;
                }
            }
            if (urls.size() > 0) {
                imageHandle = new ImageHandle(streamId, count++, urls, this.type);
                imageHandle.getMetadata().put("outputLocation", this.outputLocation);
                imageHandle.getMetadata().put("totalTaskSize", this.totalTaskSize);
                imageHandle.getMetadata().put("emitTime", System.currentTimeMillis());
            }
        }
        return imageHandle;
    }

    @Override
    public void onSignal(byte[] bytes) {
        Input input = new Input(bytes);
        ImageRequest request = imageRequestSerializer.read(kryo, input, ImageRequest.class);
        logger.info("Receive image process request: \n {}", request.toString());
        this.locations.clear();
        this.streamId = request.getStreamId();
        this.locations.add(request.getInputLocation());
        this.outputLocation = request.getOutputLocation();
        this.type = BoltOperationType.getOpType(request.getProcessType());
        partionTasks();
    }

    /**
     * Lists all files in the specified location. If the location itself is a
     * file the location will be the only object in the result. If the location
     * is a directory (or AQS S3 prefix) the result will contain all files in
     * the directory. Only files with correct extensions will be listed!
     *
     * @param location
     * @return
     */
    private List<String> expand(String location) {
        FileConnector fl = connectorHolder.getConnector(location);

        if (fl != null) {
            fl.setExtensions(new String[]{".jpg", ".JPG", ".jpeg", ".JPEG",
                    ".png", ".PNG", ".gif", ".GIF", ".bmp", ".BMP"});
            try {
                fl.moveTo(location);
            } catch (IOException e) {
                logger.warn("Unable to move to " + location + " due to: "
                        + e.getMessage());
                return new ArrayList<String>();
            }
            return fl.list();
        } else
            return new ArrayList<String>();
    }
}
