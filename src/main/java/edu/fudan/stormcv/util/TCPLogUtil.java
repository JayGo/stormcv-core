package edu.fudan.stormcv.util;

import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TCPLogUtil {
    public static boolean loadProperties() {
        File proDir = new File("./Log4jForCaptrueDispatcher");
        if (!proDir.exists()) {
            if (!proDir.mkdir())
                return false;
        }

        if (!proDir.isDirectory()) {
            if (!proDir.delete())
                return false;
            else {
                if (!proDir.mkdir())
                    return false;
                else
                    return true;
            }
        }

        File pro = new File(proDir.getName() + "/log4j.properties");
        FileOutputStream fout = null;

        if (!pro.exists()) {
            try {
                pro.createNewFile();
                pro.setWritable(true);
                fout = new FileOutputStream(pro);

//				String content[] = { "log4j.rootLogger = info, appendConsole, appendRollingFile\n",
//						"\n",
//						"log4j.appender.appendConsole = org.apache.log4j.ConsoleAppender\n",
//						"log4j.appender.appendConsole.layout = org.apache.log4j.PatternLayout\n",
//						"log4j.appender.appendConsole.layout.ConversionPattern = [%d{yyyy-MM-dd HH:mm:ss}]--[%t] [%p] -%l -%m%n%n",
//						"\n",
//						"log4j.appender.appendRollingFile = org.apache.log4j.RollingFileAppender\n",
//						"log4j.appender.appendRollingFile.encoding = UTF-8\n",
//						"log4j.appender.appendRollingFile.layout = org.apache.log4j.PatternLayout\n",
//						"log4j.appender.appendRollingFile.layout.ConversionPattern = [%d{yyyy-MM-dd HH:mm:ss}]--[%t] [%p] -%l -%m%n%n",
//						"\n",
//						"log4j.appender.appendRollingFile.Append = false\n",
//						"log4j.appender.appendRollingFile.File = "+proDir+"/log" };

                String content[] = {"log4j.rootLogger = info, appendRollingFile\n",
                        "\n",
                        "log4j.appender.appendRollingFile = org.apache.log4j.RollingFileAppender\n",
                        "log4j.appender.appendRollingFile.encoding = UTF-8\n",
                        "log4j.appender.appendRollingFile.layout = org.apache.log4j.PatternLayout\n",
                        "log4j.appender.appendRollingFile.layout.ConversionPattern = [%d{yyyy-MM-dd HH:mm:ss}]--[%t] [%p] -%l -%m%n%n",
                        "\n",
                        "log4j.appender.appendRollingFile.Append = false\n",
                        "log4j.appender.appendRollingFile.File = " + proDir + "/log"

                };

                for (String s : content) {
                    fout.write(s.getBytes());
                }

                fout.flush();
                fout.close();

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        try {
            PropertyConfigurator.configure(pro.getCanonicalPath());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }
}
