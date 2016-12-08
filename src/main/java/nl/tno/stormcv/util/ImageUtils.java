package nl.tno.stormcv.util;

import java.awt.Color;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import nl.tno.stormcv.model.Frame;

import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;

import edu.fudan.lwang.FGEHelper;

/**
 * A utility class to convert images to bytes and vice-versa which is primarily
 * used for serialization from/to Tuple's
 * 
 * @author Corne Versloot
 */
public class ImageUtils {

	static {
		try {
			NativeUtils.loadOpencvLib();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Color[] colors = new Color[] { Color.RED, Color.BLUE,
			Color.GREEN, Color.PINK, Color.YELLOW, Color.CYAN, Color.MAGENTA };

	/**
	 * Converts an image to byte buffer representing PNG (bytes as they would
	 * exist on disk)
	 * 
	 * @param image
	 * @param encoding
	 *            the encoding to be used, one of: png, jpeg, bmp, wbmp, gif
	 * @return byte[] representing the image
	 * @throws IOException
	 *             if the bytes[] could not be written
	 */
	public static byte[] imageToBytes(BufferedImage image, String encoding)
			throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(image, encoding, baos);
		return baos.toByteArray();
	}

	/**
	 * Converts the provided byte buffer into an BufferedImage
	 * 
	 * @param buf
	 *            byte[] of an image as it would exist on disk
	 * @return
	 * @throws IOException
	 */
	public static BufferedImage bytesToImage(byte[] buf) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		return ImageIO.read(bais);
	}

	/**
	 * Converts a given image into grayscalse
	 * 
	 * @param src
	 * @return
	 */
	public static BufferedImage convertToGray(BufferedImage src) {
		ColorConvertOp grayOp = new ColorConvertOp(
				ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
		return grayOp.filter(src, null);
	}

	/**
	 * Converts a given image into gray
	 * 
	 * @param src
	 * @return
	 * @throws IOException
	 */
	public static BufferedImage convertToGray2(BufferedImage src)
			throws IOException {

		int width = src.getWidth();
		int height = src.getHeight();

		BufferedImage grayImage = new BufferedImage(width, height,
				BufferedImage.TYPE_BYTE_GRAY);
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				int rgb = src.getRGB(i, j);
				grayImage.setRGB(i, j, rgb);
			}
		}
		return grayImage;
	}

	public static BufferedImage toGray(BufferedImage src) {
		return new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY),
				null).filter(src, null);
	}

	/**
	 * Creates a Mat object for the image in the provided frame
	 * 
	 * @param frame
	 * @return Mat object representing the image of type
	 *         Highgui.CV_LOAD_IMAGE_COLOR
	 * @throws IOException
	 *             if the image cannot be read or converted into binary format
	 */
	public static Mat frameToMat(Frame frame) throws IOException {
		if (frame.getImage() == null)
			throw new IOException("Frame does not contain an image");
		return bytesToMat(frame.getImageBytes());
	}

	/**
	 * Creates a Mat object for the image in the provided frame
	 * 
	 * @param image
	 *            the image to be converted to mat
	 * @param imageType
	 *            the encoding to use, see {@link Frame}
	 * @return Mat object representing the image of type
	 *         Highgui.CV_LOAD_IMAGE_COLOR
	 * @throws IOException
	 *             if the image cannot be read or converted into binary format
	 */
	public static Mat imageToMat(BufferedImage image, String imageType)
			throws IOException {
		MatOfByte mob = new MatOfByte(ImageUtils.imageToBytes(image, imageType));
		return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
	}

	/**
	 * creates a Mat object directly from a set of bytes
	 * 
	 * @param bytes
	 *            binary representation of an image
	 * @return Mat object of type Highgui.CV_LOAD_IMAGE_COLOR
	 */
	public static Mat bytesToMat(byte[] bytes) {
		MatOfByte mob = new MatOfByte(bytes);
		return Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
	}

	/**
	 * Creates a byte representation of the provided mat object encoded using
	 * the imageType
	 * 
	 * @param mat
	 * @param imageType
	 * @return
	 */
	public static byte[] matToBytes(Mat mat, String imageType) {
		MatOfByte buffer = new MatOfByte();
		Highgui.imencode("." + imageType, mat, buffer);
		return buffer.toArray();
	}

	/**************************************************************************/

	/**
	 * Convert a bufferedImage to byte[] using opencv
	 * 
	 * @param image
	 * @param width
	 * @param height
	 * @return
	 * @throws IOException
	 */
	public static byte[] imageToBytesWithOpenCV(BufferedImage image)
			throws IOException {
		Mat mat = bufferedImageToMat(image);
		if (mat != null)
			return matToBytes(mat);
		else
			return null;
	}

	/**
	 * Covert a opencv Mat to byte[]
	 * 
	 * @param in
	 * @return
	 */
	public static byte[] matToBytes(Mat in) {
		byte[] out = new byte[in.width() * in.height() * (int) in.elemSize()];
		in.get(0, 0, out);
		return out;
	}

	/**
	 * Convert the raw byte[] to a bufferedImage
	 * 
	 * @param bytes
	 * @param width
	 * @param height
	 * @param type
	 * @return
	 */
	public static BufferedImage bytesToBufferedImage(byte[] bytes, int width,
			int height, int type) {
		BufferedImage bufferedImage = new BufferedImage(width, height, type);
		bufferedImage.getRaster().setDataElements(0, 0, width, height, bytes);
		return bufferedImage;
	}

	public static BufferedImage matBytesToBufferedImage(byte[] bytes,
			int width, int height, int type) {
		Mat in = bytesToMat(bytes, width, height, type);
		BufferedImage image = matToBufferedImage(in);
		return image;
	}

	// public static byte[] matBytesToEncodedBytes(byte[] bytes, String
	// encoding) {
	// try {
	// BufferedImage image = ImageIO.read(new ByteArrayInputStream(bytes));
	// System.out.println("mattojpegbytes:" + image.getWidth());
	// ByteArrayOutputStream baos = new ByteArrayOutputStream();
	// ImageIO.write(image, encoding, baos);
	// return baos.toByteArray();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// return null;
	// }

	/**
	 * Convert byte[] to opencv Mat
	 * 
	 * @param in
	 * @param width
	 * @param height
	 * @return
	 */
	public static Mat bytesToMat(byte[] in, int width, int height, int type) {
		Mat out = Mat.eye(height, width, type);
		out.put(0, 0, in);
		return out;
	}

	/**
	 * Convert an opencv Mat to a bufferedImage
	 * 
	 * @param in
	 * @return
	 */
	public static BufferedImage matToBufferedImage(Mat in) {
		MatOfByte byteMat = new MatOfByte();

		Highgui.imencode(".jpg", in, byteMat);

		byte[] bytes = byteMat.toArray();

		ByteArrayInputStream input = new ByteArrayInputStream(bytes);

		BufferedImage img = null;

		try {
			img = ImageIO.read(input);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return img;
	}

	// public static BufferedImage matToBufferedImage(Mat in) {
	// BufferedImage out;
	// byte[] data = matToBytes(in);
	//
	// int type;
	// if (in.channels() == 1) {
	// System.out.println("output is gray.....");
	// type = BufferedImage.TYPE_BYTE_GRAY;
	// }
	// else {
	// System.out.println("output is .....");
	// type = BufferedImage.TYPE_3BYTE_BGR;
	// }
	//
	// out = new BufferedImage(in.width(), in.height(), type);
	// out.getRaster().setDataElements(0, 0, in.width(), in.height(), data);
	// return out;
	// }

	/**
	 * Convert a bufferedImage to an opencv Mat
	 * 
	 * @param in
	 * @return
	 */
	public static Mat bufferedImageToMat(BufferedImage in) {
		Mat out = null;
		if (in.getType() == BufferedImage.TYPE_3BYTE_BGR) {
			out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
		}
		else if (in.getType() == BufferedImage.TYPE_4BYTE_ABGR
				|| in.getType() == BufferedImage.TYPE_4BYTE_ABGR_PRE) {
			out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC4);
		}
		else if (in.getType() == BufferedImage.TYPE_BYTE_GRAY) {
			out = new Mat(in.getHeight(), in.getWidth(), CvType.CV_8UC3);
		}
		else {
			return out;
		}
		byte[] data = ((DataBufferByte) in.getRaster().getDataBuffer())
				.getData();
		out.put(0, 0, data);
		return out;
	}

}
