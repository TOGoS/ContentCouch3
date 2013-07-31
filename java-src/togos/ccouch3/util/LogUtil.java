package togos.ccouch3.util;

import java.util.Date;

import togos.ccouch3.FileInfo.FileType;

public class LogUtil
{
	private LogUtil() { }
	
	public static String formatStorageLogEntry(
		Date date, FileType fileType, String path, String urn
	) {
		return
			"[" + date.toString() + "] Uploaded\n" +
			fileType.niceName + " '" + path + "' = " + urn;
	}
}
