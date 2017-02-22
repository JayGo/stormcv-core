package nl.tno.stormcv.operation;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.opencv.core.Mat;
import org.opencv.core.MatOfInt;
import org.opencv.core.MatOfFloat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;


import edu.fudan.lwang.codec.OperationHandler;


import nl.tno.stormcv.model.Descriptor;
import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.serializer.FeatureSerializer;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;

/**
 * Operation to calculate the color histogram of a {@link Frame} and returns a
 * {@link Feature} with a histogram per color channel.
 * 
 * @author Corne Versloot
 */

public class ColorHistogramOp extends OpenCVOp<CVParticle> implements
		ISingleInputOperation<CVParticle> {

	private static final long serialVersionUID = -5543735411296339252L;
	private String stream;
	private int[] chansj = new int[] { 0, 1, 2 };
	private int[] histsizej = new int[] { 255, 255, 255 };
	private float[] rangesj = new float[] { 0, 256, 0, 256, 0, 256 };
	private Boolean outputFrame = false;
	@SuppressWarnings("rawtypes")
	private CVParticleSerializer serializer = new FeatureSerializer();

	public ColorHistogramOp(String stream) {
		this.stream = stream;
	}

	public ColorHistogramOp outputFrame(Boolean outFrame) {
		this.outputFrame = outFrame;
		if (outputFrame) {
			this.serializer = new FrameSerializer();
		} else {
			this.serializer = new FeatureSerializer();
		}
		return this;
	}

	/**
	 * Configure the HistorgramOperation. The default is set for use of RGB
	 * images
	 * 
	 * @param chans
	 *            list with channal id's default = {0, 1, 2}
	 * @param histsize
	 *            for each channel the number of bins to use, default = {255,
	 *            255 ,255}
	 * @param ranges
	 *            for each channel the min. and max. values present, default =
	 *            {0, 256, 0, 256, 0, 256 }
	 * @see <a
	 *      href="http://docs.opencv.org/2.4.8/modules/imgproc/doc/histograms.html">OpenCV
	 *      Documentation</a>
	 */
	public ColorHistogramOp configure(int[] chans, int[] histsize,
			float[] ranges) {
		chansj = chans;
		histsizej = histsize;
		rangesj = ranges;
		return this;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void prepareOpenCVOp(Map stormConf, TopologyContext context)
			throws Exception {
	}

	@Override
	public List<CVParticle> execute(CVParticle input) throws Exception {
		Frame sf = (Frame) input;
		BufferedImage sfimage = sf.getImage();
		MatOfByte mob = new MatOfByte(sf.getImageBytes());
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
		Mat hist = new Mat();

		MatOfInt chans;
		MatOfInt histsize;
		MatOfFloat ranges;

		List<Mat> images = new ArrayList<Mat>();

		ArrayList<CVParticle> result = new ArrayList<CVParticle>();
		ArrayList<Descriptor> hist_descriptors = new ArrayList<Descriptor>();

		Rectangle box = new Rectangle(0, 0, (int) image.size().width,
				(int) image.size().height);

		images.add(image);

		for (int i = 0; i < chansj.length; i++) {
			chans = new MatOfInt(chansj[i]);
			histsize = new MatOfInt(histsizej[i]);
			ranges = new MatOfFloat(rangesj[i * 2], rangesj[i * 2 + 1]);
			Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

			float[] tmp = new float[1];
			int rows = (int) hist.size().height;
			float[] values = new float[rows];
			int c = 0;
			float uh = image.height() / chansj.length;
			float uw = image.width() / rows;
			float maxHight = 0;
			for (int r = 0; r < rows; r++) // loop over rows/columns
			{
				hist.get(r, c, tmp);
				values[r] = tmp[0];
				if (tmp[0] > maxHight)
					maxHight = tmp[0];
			}
			// draw colorhistogm on the frame image
			Graphics2D graphics = sfimage.createGraphics();
			for (int r = 0; r < rows; ++r) {
				Color color = new Color((i == 0 ? r : 0), (i == 1 ? r : 0),
						(i == 2 ? r : 0));
				graphics.setColor(color);
				graphics.drawRect((int) (r * uw),
						(int) ((i + 1) * uh - values[r] / maxHight
								* uh), (int) uw,
						(int) (values[r] / maxHight * uh));
			}
			hist_descriptors.add(new Descriptor(input.getStreamId(), input
					.getSequenceNr(), box, 0, values));
		}

		sf.setImage(sfimage);
		Feature feature = new Feature(input.getStreamId(),
				input.getSequenceNr(), stream, 0, hist_descriptors, null);

		// add features to result
		if (hist_descriptors.size() > 0) {
			if (outputFrame) {
				sf.getFeatures().add(feature);
				result.add(sf);
			} else {
				result.add(feature);
			}
		}
		return result;
	}

	@Override
	public void deactivate() {

	}

	@SuppressWarnings("unchecked")
	@Override
	public CVParticleSerializer<CVParticle> getSerializer() {
		return this.serializer;
	}


	@Override
	public String getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CVParticle> execute(CVParticle particle, OperationHandler codecHandler) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}




}
