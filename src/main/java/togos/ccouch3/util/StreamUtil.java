package togos.ccouch3.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

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

	public static void copy(InputStream in, OutputStream out)
		throws IOException
	{
		int r;
		byte[] buffer = new byte[1024*128];
		while( (r = in.read(buffer)) > 0 ) {
			out.write(buffer, 0, r);
		}
	}
	
	public static byte[] slurp(InputStream in) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		copy(in, baos);
		return baos.toByteArray();
	}
	
	public static Consumer<byte[]> outputStreamToConsumer(final OutputStream os) {
		return new Consumer<byte[]>() {
			@Override public void accept(byte[] value) {
				try {
					os.write(value);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}
	
	public static PrintStream toPrintStream(Object os) {
		if( os == null ) return null;
		if( os instanceof PrintStream ) return (PrintStream)os;
		if( os instanceof OutputStream ) {
			try {
				return new PrintStream((OutputStream)os, false, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("Don't know how to make PrintStream from "+os);
	}
}
