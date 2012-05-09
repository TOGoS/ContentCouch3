package togos.blob.util;

import java.io.UnsupportedEncodingException;

// import togos.blob.ByteBlob;
import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;

public class BlobUtil
{
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	
	public static final byte[] bytes( String str ) {
		try {
			return str.getBytes("UTF-8");
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static final ByteChunk byteChunk( String str ) {
		return new SimpleByteChunk( bytes(str) );
	}
	
	public static final String string( byte[] arr, int offset, int len ) {
		try {
			return new String( arr, offset, len, "UTF-8" );
		} catch( UnsupportedEncodingException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public static final String string( ByteChunk bc ) {
		return string( bc.getBuffer(), bc.getOffset(), bc.getSize() );
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
	
	public static final boolean equals( ByteChunk c1, ByteChunk c2 ) {
		if( c1.getSize() != c2.getSize() ) return false;
		
		int o1 = c1.getOffset(), o2 = c2.getOffset();
		byte[] b1 = c1.getBuffer(), b2 = c2.getBuffer();
		for( int j=c1.getSize()-1; j>=0; --j ) {
			if( b1[j+o1] != b2[j+o2] ) return false;
		}
		return true;
	}
	
	public static final boolean equals( byte[] b1, byte[] b2 ) {
		if( b1.length != b2.length ) {
			return false;
		}
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
	
	/**
	 * Don't compare blobs this way.
	 * TODO: Implement more efficiently
	 */
	/*
	public static boolean equals( ByteBlob b1, ByteBlob b2 ) {
		if( b1.getSize() != -1 && b2.getSize() != -1 && b1.getSize() != b2.getSize() ) return false;
		
		int r1;
		ByteBlobInputStream s1 = new ByteBlobInputStream(b1.chunkIterator());
		ByteBlobInputStream s2 = new ByteBlobInputStream(b2.chunkIterator());
		do {
			r1 = s1.read();
			if( s2.read() != r1 ) return false;
		} while( r1 != -1 );
		return true;
	}
	*/
}
