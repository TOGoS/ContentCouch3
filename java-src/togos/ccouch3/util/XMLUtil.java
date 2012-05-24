package togos.ccouch3.util;

public class XMLUtil
{
	public static String xmlEscapeText( String text ) {
		return text.replaceAll("&","&amp;").replaceAll("<", "&lt;").replaceAll(">","&gt;");
	}

	public static String xmlEscapeAttribute( String text ) {
		return xmlEscapeText( text ).replaceAll("\"", "&quot;");
	}
}
