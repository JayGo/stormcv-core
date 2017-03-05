package edu.fudan.stormcv.util.connector;

import edu.fudan.stormcv.StormCVConfig;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.storm.shade.org.yaml.snakeyaml.util.UriEncoder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static edu.fudan.stormcv.StormCVConfig.FTP_PASSWORD;
import static edu.fudan.stormcv.StormCVConfig.FTP_USERNAME;

/**
 * A {@link FileConnector} implementation used to access FTP sites. The FTP_USERNAME and FTP_PASSWORD fields must be
 * set in  the {@link StormCVConfig} to provide credentials for the ftp site. If no credentials are provided the connector
 * will attempt to access the ftp server locations without credentials. This connector is used for locations starting with <b>ftp://</b>
 *
 * @author Corne Versloot
 */
public class FtpConnector implements FileConnector {
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static final String SCHEMA = "ftp";
    private String[] extensions;
    private URI location;
    private String username;
    private String password;
    private FTPClient client;

    public FtpConnector() {

    }

    public FtpConnector(String name, String pass) {
        this.username = name;
        this.password = pass;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf) throws DatatypeConfigurationException {
        if (stormConf.containsKey(FTP_USERNAME)) username = (String) stormConf.get(FTP_USERNAME);
        if (stormConf.containsKey(FTP_PASSWORD)) password = (String) stormConf.get(FTP_PASSWORD);
    }

    @Override
    public FileConnector setExtensions(String[] extensions) {
        this.extensions = extensions;
        return this;
    }

    @Override
    public void moveTo(String loc) throws IOException {
        try {
            this.location = new URI(UriEncoder.encode(loc));
            checkAndConnect();
            int code = client.cwd(location.getPath());
            if (code == 250) return;
            code = client.cwd(location.getPath().substring(0, location.getPath().lastIndexOf('/')));
        } catch (Exception e) {
            logger.warn("Unable to move to " + location + " due to: " + e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public boolean copyFile(File localFile, boolean delete) throws IOException {
        checkAndConnect();
        Boolean flag = client.storeFile(location.getPath()
                        .substring(location.getPath().lastIndexOf('/') + 1),
                new FileInputStream(localFile));
        if (flag) {
            logger.info("File " + location.getPath()
                    .substring(location.getPath().lastIndexOf('/') + 1) + " upload succefully!");
        } else {
            logger.info("File " + location.getPath()
                    .substring(location.getPath().lastIndexOf('/') + 1) + " upload failed!");
        }
        if (delete) {
            localFile.delete();
        }
        return flag;
    }

    @Override
    public List<String> list() {
        List<String> result = new ArrayList<String>();
        try {
            checkAndConnect();
            if (!location.getPath().equals(client.printWorkingDirectory())) {
                moveTo(location);
            }

            FTPFile[] files = client.listFiles();
            list:
            for (FTPFile file : files) {
                for (String ext : extensions) {
                    if (file.getName().endsWith(ext)) {
                        result.add(SCHEMA + "://" + location.getHost() + (location.getPort() > -1 ? ":" + location.getPort() : "") + client.printWorkingDirectory() + "/" + file.getName());
                        continue list;
                    }
                }
            }
        } catch (IOException ioe) {
            logger.warn("Unable to list ftp directory due to : " + ioe.getMessage());
        }
        return result;
    }

    @Override
    public String getProtocol() {
        return SCHEMA;
    }

    @Override
    public File getAsFile() throws IOException {
        checkAndConnect();
        File tmpFile = File.createTempFile("" + location.getPath().hashCode(), location.getPath().substring(location.getPath().lastIndexOf(".")));
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(tmpFile);
            client.retrieveFile(location.getPath().substring(location.getPath().lastIndexOf('/') + 1), fos);
        } catch (Exception e) {
            Utils.sleep(2000);
            checkAndConnect();
            fos = new FileOutputStream(tmpFile);
            client.retrieveFile(location.getPath().substring(location.getPath().lastIndexOf('/') + 1), fos);
        }
        fos.close();
        return tmpFile;
    }

    @Override
    public FileConnector deepCopy() {
        FtpConnector adaptor = new FtpConnector(username, password);
        adaptor.moveTo(location);
        return adaptor;
    }

    private void checkAndConnect() throws IOException {
        if (client == null) client = new FTPClient();
        if (!client.isConnected()) {
            if (location.getPort() == -1) {
                client.connect(location.getHost());
            } else {
                client.connect(location.getHost(), location.getPort());
            }
            if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                client.disconnect();
                throw new IOException("Exception in connecting to FTP Server");
            }

            if (username != null && password != null) {
                boolean isLogin = client.login(username, password);
                if (!isLogin) logger.info("Ftp login failed!");
            }
            client.setFileType(FTP.BINARY_FILE_TYPE);
        }
    }

    private void moveTo(URI uri) {
        this.location = uri;
    }
}
