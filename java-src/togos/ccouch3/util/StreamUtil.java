package togos.ccouch3.util;

import java.io.BufferedInputStream;
import java.io.InputStream;

public class StreamUtil
{
	public static InputStream markableInputStream( InputStream is ) {
		return is.markSupported() ? is : new BufferedInputStream(is);
	}
}
