//package nl.tno.stormcv.util;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.URL;
//
//import nl.tno.stormcv.operation.OpenCVOp;
//
//import org.apache.storm.utils.Utils;
//
///**
// * A utility class used to load platform dependent (OpenCV) libraries and other
// * resources and is used by {@link OpenCVOp} implementations. Libraries must be
// * present on the classpath for this utility class to function properly. If the
// * library resides within a jar file it will be extracted to the local tmp
// * directory before it is loaded.
// *
// * The project is shipped with the following OpenCV 2.4.8 binaries:
// * <ul>
// * <li>Windows dll's both 32 and 64 bit</li>
// * <li>Mac OS dylib, 64 bit only</li>
// * <li>Ubuntu 12.04 LTS so 64 bit only</li>
// * </ul>
// *
// * It is possible to build your own libraries on the platform of your choosing,
// * see: http://docs.opencv.org/doc/tutorials/introduction/desktop_java/
// * java_dev_intro.html
// *
// * @author Corne Versloot
// */
//public class NativeUtils {
//
//	/**
//	 * Loads the openCV library independent of OS and architecture
//	 *
//	 * @throws RuntimeException
//	 *             when the library cannot be found
//	 * @throws IOException
//	 *             when the library could not be extracted or loaded
//	 */
//	public static void load() throws RuntimeException, IOException {
//		try {
//			System.loadLibrary("opencv_java248");
//		} catch (UnsatisfiedLinkError e) {
//			String opencvLib = "/" + getOpenCVLib();
//			load(opencvLib);
//		}
//	}
//
//	public static boolean loadBayesFGELibrary() {
//		String bayesLib = "/usr/local/storm/jniLib/bayes_fge.so";
//		boolean result = false;
//		try {
////			load(bayesLib);
////			URL url = NativeUtils.class.getResource(bayesLib);
////			File file =  new File(url.getFile());
//			System.load(bayesLib);
//			result = true;
//		} catch (UnsatisfiedLinkError e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public static boolean loadCudaLibrary() {
//		boolean result = false;
//		try {
//			System.load("/usr/local/cuda/lib64/libcudart.so");
//			result = true;
//		} catch (UnsatisfiedLinkError e) {
//
//		} catch (Exception e) {
//
//		}
//		return result;
//	}
//
//	/**
//	 * Load Opencv Lib by jkyan
//	 *
//	 * @throws IOException
//	 */
//	public static void loadOpencvLib() throws IOException {
//		// loadSelfJarLib(getOpenCVLib());
//		loadSelfJarLib(getOpenCVLib_13());
//	}
//
//	/**
//	 * Extract the file in jar to temp and loading it by jkyan
//	 *
//	 * @param name
//	 * @throws IOException
//	 */
//	public static void loadSelfJarLib(String name) throws IOException {
//		if (name.charAt(0) != '/')
//			name = "/" + name;
//		String tempLocation = getSelfJarDenpendencyFileTmp(name);
//		System.load(tempLocation);
//	}
//
//	/**
//	 * Extract the file in jar to temp by jkyan
//	 *
//	 * @param name
//	 * @return the temp file path
//	 * @throws IOException
//	 */
//	public static String getSelfJarDenpendencyFileTmp(String name)
//			throws IOException {
//		if (name.charAt(0) != '/')
//			name = "/" + name;
//		InputStream in = NativeUtils.class.getResourceAsStream(name);
//		if (in == null) {
//			throw new FileNotFoundException("File " + name + " Found!");
//		}
//		byte[] buffer = new byte[1024];
//		int read = -1;
//		// every user extract the linked so file to his temp file to avoid
//		// confliction
//		File temp_path = new File(System.getProperty("java.io.tmpdir") + "/"
//				+ System.getProperty("user.name"));
//		if (!temp_path.exists()) {
//			temp_path.mkdirs();
//		}
//		System.out.println("tmp path:" + temp_path);
//		File temp = new File(temp_path, name);
//
//		FileOutputStream fos = new FileOutputStream(temp);
//
//		while ((read = in.read(buffer)) != -1) {
//			fos.write(buffer, 0, read);
//		}
//
//		fos.close();
//		in.close();
//
//		return (temp.getAbsolutePath());
//	}
//
//	/**
//	 * Loads the openCV library represented by the given name (should be present
//	 * on the classpath).
//	 *
//	 * @throws RuntimeException
//	 *             when the library cannot be found
//	 * @throws IOException
//	 *             when the library could not be extracted or loaded
//	 */
//	public static void load(String name) throws RuntimeException, IOException {
//		if (!name.startsWith("/"))
//			name = "/" + name;
//		File libFile = NativeUtils.getAsLocalFile(name);
//		Utils.sleep(500); // wait a bit to be sure the library is ready to be
//							// read
//		System.load(libFile.getAbsolutePath());
//	}
//
//	/**
//	 * Determines the OS dependent opencv library to load.
//	 *
//	 * @return the proper name of the library
//	 * @throws RuntimeException
//	 */
//	private static String getOpenCVLib() throws RuntimeException {
//		// Detect 32bit vs 64 bit
//		String arch = (System.getProperty("os.arch").toLowerCase()
//				.contains("64") ? "64" : "32");
//
//		// Detect OS
//		String osName = System.getProperty("os.name").toLowerCase();
//		if (osName.contains("win")) {
//			return "win" + arch + "_opencv_java248.dll";
//		}
//		else if (osName.contains("mac")) {
//			return "mac" + arch + "_opencv_java248.dylib";
//		}
//		else if (osName.contains("linux") || osName.contains("nix")) {
//			return "linux" + arch + "_opencv_java248.so";
//		}
//		else
//			throw new RuntimeException("Unable to determine proper OS!");
//	}
//
//	private static String getOpenCVLib_13() throws RuntimeException {
//		// Detect 32bit vs 64 bit
//		String arch = (System.getProperty("os.arch").toLowerCase()
//				.contains("64") ? "64" : "32");
//
//		// Detect OS
//		String osName = System.getProperty("os.name").toLowerCase();
//		if (osName.contains("win")) {
//			return "win" + arch + "_opencv_java2413.dll";
//		}
//		else if (osName.contains("mac")) {
//			return "mac" + arch + "_opencv_java2413.dylib";
//		}
//		else if (osName.contains("linux") || osName.contains("nix")) {
//			return "linux" + arch + "_opencv_java2413.so";
//		}
//		else
//			throw new RuntimeException("Unable to determine proper OS!");
//	}
//
//	/**
//	 * Attempts to first extract the library at path to the tmp dir and load it
//	 *
//	 * @param name
//	 * @throws IOException
//	 */
//	public static File getAsLocalFile(String name) throws IOException {
//		if (!name.startsWith("/"))
//			name = "/" + name;
//
//		URL url = NativeUtils.class.getResource(name);
//		if (url == null)
//			throw new FileNotFoundException("Unable to locate " + name);
//
//		File file = null;
//		if (url.getProtocol().equals("jar")) {
//			file = extractTmpFileFromJar(name, false);
//		}
//		else {
//			file = new File(url.getFile());
//		}
//		return file;
//	}
//
//	/**
//	 * Tries to extract the resource at path to the systems tmp dir
//	 *
//	 * @param path
//	 *            the path or file of the resource to be extracted
//	 * @param deleteOnExit
//	 * @return the file the resource was written to
//	 * @throws IOException
//	 *             in case the resource cannot be read or written to tmp
//	 */
//	public static File extractTmpFileFromJar(String path, boolean deleteOnExit)
//			throws IOException {
//
//		// Prepare temporary file
//		File temp = File.createTempFile("abcd", "efgh");
//		temp.delete();
//		temp = new File(temp.getParentFile().getAbsolutePath() + path);
//		if (deleteOnExit)
//			temp.deleteOnExit();
//		if (temp.exists())
//			return temp;
//
//		// Prepare buffer for data copying
//		byte[] buffer = new byte[1024];
//		int readBytes;
//
//		// Open and check input stream
//		InputStream is = NativeUtils.class.getResourceAsStream(path);
//		if (is == null) {
//			throw new FileNotFoundException("File " + path
//					+ " was not found inside JAR.");
//		}
//
//		// Open output stream and copy data between source file in JAR and the
//		// temporary file
//		OutputStream os = new FileOutputStream(temp);
//		try {
//			while ((readBytes = is.read(buffer)) != -1) {
//				os.write(buffer, 0, readBytes);
//			}
//		} finally {
//			// If read/write fails, close streams safely before throwing an
//			// exception
//			os.close();
//			is.close();
//		}
//
//		return temp;
//	}
//
//}