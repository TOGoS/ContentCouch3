package togos.ccouch3.util;

import java.util.Date;

import togos.ccouch3.FSObjectType;

public class LogUtil
{
	private LogUtil() { }
	
	public static String formatStorageLogEntry(
		Date date, FSObjectType fileType, String path, String urn
	) {
		return
			"[" + date.toString() + "] Uploaded\n" +
			fileType.niceName + " '" + path + "' = " + urn;
	}
}
