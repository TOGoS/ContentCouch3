package togos.blob.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import togos.blob.ByteChunk;
import togos.blob.InputStreamable;

public class BlobUtil
{
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	
	public static final ByteChunk byteChunk( byte[] bytes ) {
		return (bytes.length == 0) ? SimpleByteChunk.EMPTY : new SimpleByteChunk( bytes );
	}
	
	public static final ByteChunk byteChunk( String str ) {
		return byteChunk(bytes(str));
	}
	
	public static void pipe( InputStream is, OutputStream os ) throws IOException {
		byte[] buf = new byte[65536];
		int z;
		while( (z = is.read(buf)) > 0 ) {
			os.write(buf, 0, z);
		}
	}
	
	public static void pipe( InputStreamable isa, OutputStream os ) throws IOException {
		InputStream is = isa.openInputStream();
		try {
			pipe(is, os);
		} finally {
			is.close();
		}
	}
	
	public static final byte[] bytes( String str ) {
		try {
			return str.getBytes("UTF-8");
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static int toInt(long l) {
		if( l > Integer.MAX_VALUE || l < Integer.MIN_VALUE ) throw new NumberFormatException("Cannot convert long to int because it is TOO BIG: "+l);
		return (int)l;
	}
	
	public static int chunkLength(ByteChunk b) {
		return toInt(b.getSize());
	}
	
	public static final String string( byte[] arr, int offset, int len ) {
		try {
			return new String( arr, offset, len, "UTF-8" );
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static final String string( ByteChunk chunk ) {
		return string( chunk.getBuffer(), chunk.getOffset(), toInt(chunk.getSize()) );
	}

	/**
	 * Should be compatible with Arrays.hashCode( byte[] data ),
	 * which is supposedly compatible with List<Byte>#hashCode.
	 */
	public static final int hashCode(byte[] data) {
		return hashCode(data,0,data.length);
	}
	
	public static final int hashCode(byte[] data, int offset, int length) {
		int hashCode = 1;
		for( int i=0; i<length; ++i ) {
			hashCode = 31*hashCode + data[i+offset];
		}
		return hashCode;
	}
	
	public static final boolean equals( byte[] b1, int o1, byte[] b2, int o2, int len ) {
		for( int i=0; i<len; ++i ) {
			if( b1[o1++] != b2[o2++] ) return false;
		}
		return true;
	}
	
	public static final boolean equals( ByteChunk c1, ByteChunk c2 ) {
		if( c1.getSize() != c2.getSize() ) return false;
		return equals( c1.getBuffer(), c1.getOffset(), c2.getBuffer(), c2.getOffset(), toInt(c1.getSize()) );
	}
	
	public static final boolean equals( byte[] b1, byte[] b2 ) {
		if( b1.length != b2.length ) return false;
		for( int i=0; i<b1.length; ++i ) {
			if( b1[i] != b2[i] ) return false;
		}
		return true;
	}
	
	public static final byte[] slice( byte[] buf, int begin, int length ) {
		if( length <= 0 ) return EMPTY_BYTE_ARRAY;
		
		byte[] r = new byte[length];
		for( int i=0; i<length; ++i ) {
			r[i] = buf[i+begin];
		}
		return r;
	}
	
	public static ByteChunk slice(ByteChunk bc, long offset, long length) {
		return SimpleByteChunk.get(bc.getBuffer(), bc.getOffset()+offset, Math.min(bc.getSize() - offset, length));
	}
	
	public static final int contentHashCode( Object c ) {
		if( c == null ) return 0;
		if( c instanceof byte[] ) return hashCode( (byte[])c );
		return c.hashCode();
	}
	
	public static final boolean contentEquals( Object c1, Object c2 ) {
		if( c1 == null && c2 == null ) return true;
		if( c1 == null || c2 == null ) return false;
		if( c1 instanceof byte[] && c2 instanceof byte[] ) {
			return equals( (byte[])c1, (byte[])c2 );
		}
		return c1.equals(c2);
	}
}
