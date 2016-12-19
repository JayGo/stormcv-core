package nl.tno.stormcv.util;

public class HashUtil {
	public static String getStreamIdByAddr(String addr) {
		return addr.hashCode() < 0 ? ("" + addr.hashCode()).replace("-", "a") : addr.hashCode() + "";
	}
}
