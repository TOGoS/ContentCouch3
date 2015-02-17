package togos.ccouch3.util;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class StreamUtil
{
	public static InputStream markableInputStream( InputStream is ) {
		return is.markSupported() ? is : new BufferedInputStream(is);
	}
	
	public static void close( Closeable c ) {
		try {
			c.close();
		} catch( IOException e ) {
			System.err.println("Error while closing something!");
		}
	}
}
