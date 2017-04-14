package edu.fudan.stormcv.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/14/17 - 1:01 PM
 * Description:
 */
public class FileUtil {
    public static void TestAndMkdir(String location, boolean isTemp) {
        if (location.startsWith("file://")) {
            location = location.substring(7);
        }
        File file = new File(location);
        if (!file.exists()) {
            final Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
            final FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
            try {
                if (isTemp) {
                    Files.createTempDirectory(location, attr);
                } else {
                    Files.createDirectory(Paths.get(location), attr);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
