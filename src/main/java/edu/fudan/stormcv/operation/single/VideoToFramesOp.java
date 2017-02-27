package edu.fudan.stormcv.operation.single;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import edu.fudan.lwang.codec.OperationHandler;
import edu.fudan.stormcv.StormCVConfig;
import edu.fudan.stormcv.codec.JPEGImageCodec;
import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.VideoChunk;
import edu.fudan.stormcv.model.serializer.CVParticleSerializer;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.model.serializer.GroupOfFramesSerializer;
import edu.fudan.stormcv.codec.TurboJPEGImageCodec;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.GroupOfFrames;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Converts received {@link VideoChunk} objects back to {@link Frame}'s (optionally wrapped in {@link GroupOfFrames}).
 * Using video chunks consumes less bandwidth than raw frames.
 *
 * @author Corne Versloot
 */
public class VideoToFramesOp extends MediaListenerAdapter implements ISingleInputOperation<CVParticle> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private int frameSkip;
    private boolean groupOfFrames = false;
    private List<Frame> frames;
    private long seqNum;
    private String streamId;
    private String imageType = Frame.JPG_IMAGE;
    private JPEGImageCodec codec;

    /**
     * Instantiate a VideoToFrames operation by specifying the frameskip used to create the video (needed to calculate the correct
     * frameNumbers for the frames in the video. By default the frames in the video will be emitted to the next bolt individually.
     * This behavior can be changed using the groupFrames setting.
     *
     * @param frameSkip the frameskip used to create the video
     */
    public VideoToFramesOp(int frameSkip) {
        this.frameSkip = frameSkip;
    }

    /**
     * Indicates that the frames from the video must be emitted as a {@link GroupOfFrames} object rather than individual
     * frames.
     *
     * @return itself
     */
    public VideoToFramesOp groupFrames() {
        groupOfFrames = true;
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context) throws Exception {
        if (conf.containsKey(StormCVConfig.STORMCV_FRAME_ENCODING)) {
            imageType = (String) conf.get(StormCVConfig.STORMCV_FRAME_ENCODING);
        }
        this.codec = new TurboJPEGImageCodec();
    }

    @Override
    public void deactivate() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public CVParticleSerializer getSerializer() {
        if (groupOfFrames) return new GroupOfFramesSerializer();
        else return new FrameSerializer();
    }

    @Override
    public void onVideoPicture(IVideoPictureEvent event) {
        BufferedImage frame = event.getImage();
        byte[] buffer = this.codec.BufferedImageToJPEGBytes(frame);
//            byte[] buffer = ImageUtils.imageToBytes(frame, imageType);
        Frame newFrame = new Frame(streamId, seqNum + frames.size() * frameSkip, imageType, buffer, event.getTimeStamp(TimeUnit.MILLISECONDS), new Rectangle(0, 0, frame.getWidth(), frame.getHeight()));
        frames.add(newFrame);
    }


    @Override
    public String getContext() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
        List<CVParticle> result = new ArrayList<CVParticle>();
        if (!(particle instanceof VideoChunk)) return result;

        VideoChunk video = (VideoChunk) particle;
        frames = new ArrayList<>();
        seqNum = video.getSequenceNr();
        streamId = video.getStreamId();

        File tmpVideo = File.createTempFile("videochunk_", "." + video.getContainer());
        FileOutputStream fos = new FileOutputStream(tmpVideo);
        fos.write(video.getVideo());
        fos.flush();
        fos.close();
        IMediaReader reader = ToolFactory.makeReader(tmpVideo.getAbsolutePath());

        reader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
        reader.addListener(this);
        while (reader.readPacket() == null) ;
        reader.close();
        if (!tmpVideo.delete()) tmpVideo.deleteOnExit();

        if (groupOfFrames) {
            result.add(new GroupOfFrames(video.getStreamId(), video.getSequenceNr(), frames));
        } else {
            for (Frame frame : frames) result.add(frame);
        }
        return result;
    }
}
