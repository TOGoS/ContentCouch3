package togos.ccouch3.util;

public class StringUtil
{
	private StringUtil() { throw new RuntimeException("Don't instantiat me, bro!"); }
	
	public static String quote(String s) {
		return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r","\\r") + "\"";
	}
	
	public static String join(String sep, String...parts) {
		StringBuilder sb = new StringBuilder();
		for( String part : parts ) if( part != null ) {
			if( sb.length() > 0 ) sb.append(sep);
			sb.append(part);
		}
		return sb.toString();
	}
	
	public static void appendWithSeparator(StringBuilder dest, String sep, String appendMe) {
		if( appendMe == null || appendMe.isEmpty() ) return;
		if( dest.length() > 0 ) dest.append(sep);
		dest.append(appendMe);
	}
}
