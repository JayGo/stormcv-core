package edu.fudan.stormcv.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author jkyan
 * @version
 */

public class ConfigUtil {

	static Properties dbProps = new Properties();
	static File dbFile = null;
	static {
		try {
			dbFile = new File("config.properties");
			dbProps.load(new FileInputStream(dbFile.getAbsolutePath()));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * This function fetches the DB Ip from Config.properties
	 */
	public static String getDBIp() {
		return dbProps.getProperty("edu.fudan.stormcv.mysql.ip");
	}

	/*
	 * This function fetches the DBPassword from Config.properties
	 */
	public static String getDBPassword() {
		return dbProps.getProperty("edu.fudan.stormcv.mysql.password");
	}

	/*
	 * This function fetches Username from Config.properties
	 */
	public static String getDBUser() {
		return dbProps.getProperty("edu.fudan.stormcv.mysql.username");
	}
	
	/*
	 * This function fetches the DB Name from Config.properties
	 */
	public static String getDBName() {
		return dbProps.getProperty("edu.fudan.stormcv.mysql.name");
	}

	/*
	 * This function fetches the DB Port from Config.properties
	 */
	public static int getDBPort() {
		return new Integer(dbProps.getProperty("edu.fudan.stormcv.mysql.port"));
	}
}