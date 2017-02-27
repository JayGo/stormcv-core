package edu.fudan.stormcv.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/22/17 - 8:58 PM
 * Description:
 * A utility class used to load platform dependent (like OpenCV) libraries and other
 * resources. Libraries must be present on the classpath for this utility class to function properly. If the
 * library resides within a jar file it will be extracted to the local tmp directory before it is loaded.
 */
public final class LibLoader {

    public static Logger logger = LoggerFactory.getLogger(LibLoader.class);

    private static final String opencvlibName = "libopencv_java2413.so";
    private static final String defaultOpenCVLibPath = "/usr/local/opencv/share/OpenCV/java/";

    private static final String h264LibName = "libHgCodec.so";
    private static final String defaultH264LibPath = "/usr/local/LwangCodec/lib/";

    private static final String bayesFGELibName = "bayes_fge.so";
    private static final String defaultBayesFGELibPath = "/usr/local/storm/jniLib/";

    private static final String cudartLibName = "libcudart.so";
    private static final String defaultCudartLibPath = "/usr/local/cuda/lib64/";

    private static final String hgCodecLibName = "libHgCodec.so";
    private static final String defaultHgCodecLibPath = "/usr/local/LwangCodec/lib/";

    private static final String rtmpStreamerLibName = "libH264RtmpStreamer.so";
    private static final String defaultRtmpStreamerLibPath = "/usr/local/LwangCodec/lib/";

    public static boolean loadOpenCVLib() {
        return loadLibrary(defaultOpenCVLibPath, opencvlibName);
    }

    public static boolean loadH264CodecLib() {
        return loadLibrary(defaultH264LibPath, h264LibName);
    }

    public static boolean loadBayesFGELib() {
        return loadLibrary(defaultBayesFGELibPath, bayesFGELibName);
    }

    public static boolean loadCudartLib() {
        return loadLibrary(defaultCudartLibPath, cudartLibName);
    }

    public static boolean loadHgCodecLib() {
        return loadLibrary(defaultHgCodecLibPath, hgCodecLibName);
    }

    public static boolean loadRtmpStreamerLib() {
        return loadLibrary(defaultRtmpStreamerLibPath, rtmpStreamerLibName);
    }

    private static boolean loadLibrary(String path, String libName) {
        if (!path.endsWith("/")) {
            path += "/";
        }
        try {
            System.load(path + libName);
        } catch (UnsatisfiedLinkError e) {
            logger.info("Unable load " + path + libName);
            e.printStackTrace();
            try {
                loadSelfJarLib(libName);
            } catch (UnsatisfiedLinkError | IOException e1) {
                e1.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Extract the file in jar to temp and loading it by jkyan
     *
     * @param name
     * @throws IOException
     */
    private static void loadSelfJarLib(String name) throws IOException {
        if (name.charAt(0) != '/') {
            name = "/" + name;
        }
        String tempLocation = getSelfJarDenpendencyFileTmp(name, false);
        System.load(tempLocation);
    }

    /**
     * Extract the file in jar to temp by jkyan
     *
     * @param name
     * @return the temp file path
     * @throws IOException
     */
    public static String getSelfJarDenpendencyFileTmp(String name, boolean deleteOnExit)
            throws IOException {
        if (name.charAt(0) != '/') {
            name = "/" + name;
        }
        InputStream in = LibLoader.class.getResourceAsStream(name);
        if (in == null) {
            throw new FileNotFoundException("File " + name + " Found!");
        }
        byte[] buffer = new byte[1024];
        int read = -1;
        // every user extract the linked so file to his temp file to avoid
        // confliction
        File temp_path = new File(System.getProperty("java.io.tmpdir") + "/"
                + System.getProperty("user.name"));
        if (!temp_path.exists()) {
            temp_path.mkdirs();
        }
        logger.info("{} library tmp path: {}", name, temp_path);
        File temp = new File(temp_path, name);

        if (deleteOnExit) {
            temp.deleteOnExit();
        }
        if (temp.exists()) {
            in.close();
            return temp.getAbsolutePath();
        }

        FileOutputStream fos = new FileOutputStream(temp);
        while ((read = in.read(buffer)) != -1) {
            fos.write(buffer, 0, read);
        }
        fos.close();
        in.close();
        return (temp.getAbsolutePath());
    }

    /**
     * Determines the OS dependent opencv library to load.
     *
     * @return the proper name of the library
     * @throws RuntimeException
     */
    private static String getOpenCVLib_13() throws RuntimeException {
        // Detect 32bit vs 64 bit
        String arch = (System.getProperty("os.arch").toLowerCase()
                .contains("64") ? "64" : "32");
        // Detect OS
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("win")) {
            return "win" + arch + "_opencv_java2413.dll";
        } else if (osName.contains("mac")) {
            return "mac" + arch + "_opencv_java2413.dylib";
        } else if (osName.contains("linux") || osName.contains("nix")) {
            return "linux" + arch + "_opencv_java2413.so";
        } else
            throw new RuntimeException("Unable to determine proper OS!");
    }
}
