package togos.ccouch3.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {

	public static final DateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	public static final DateFormat OLDDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	static {
		DATEFORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
		OLDDATEFORMAT.setLenient(true);
	}
	
	public static String formatDate(long date) {
		return DATEFORMAT.format(new Date(date));
	}
	
	public static String formatDate(Date date) {
		return DATEFORMAT.format(date);
	}
	
	public static Date parseDate(String date) throws ParseException {
		try {
			return DATEFORMAT.parse(date);
		} catch(ParseException e) {}
		
		return OLDDATEFORMAT.parse(date);
	}
}
