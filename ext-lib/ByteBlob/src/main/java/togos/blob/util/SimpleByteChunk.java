package togos.blob.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import togos.blob.ByteBlob;
import togos.blob.ByteChunk;

public class SimpleByteChunk implements ByteChunk
{
	public static final SimpleByteChunk EMPTY = new SimpleByteChunk(new byte[0],0,0);
	
	/** @return a new SimpleByteChunk referencing a new buffer. */
	public static SimpleByteChunk copyOf( byte[] buf, int offset, int size ) {
		byte[] bu2 = new byte[size];
		SimpleByteChunk sbc = new SimpleByteChunk( bu2, 0, size );
		for( size = size-1; size >= 0; --size ) {
			bu2[size] = buf[offset+size];
		}
		return sbc;
	}
	
	public static SimpleByteChunk get( byte[] buf, long offset, long size ) {
		int _offset = BlobUtil.toInt(offset);
		int _size = BlobUtil.toInt(size);
		assert _size == size;
		assert _offset == offset;
		if( _size == 0 ) return EMPTY;
		return new SimpleByteChunk( buf, _offset, _size );
	}
	
	public static SimpleByteChunk get( byte[] buf ) {
		return get(buf, 0, buf.length);
	}
	
	public final byte[] buffer;
	public final int offset;
	public final int size;
	
	protected SimpleByteChunk( byte[] buf, int offset, int size ) {
		assert buf != null;
		assert offset >= 0;
		assert size >= 0;
		assert offset + size <= buf.length;
		this.buffer = buf;
		this.offset = offset;
		this.size = size;
	}
	
	public SimpleByteChunk( byte[] buf ) {
		this( buf, 0, buf.length );
	}
	
	public byte[] getBuffer() { return buffer; }
	public int getOffset() { return offset; }
	public long getSize() { return size; }
	
	@Override public InputStream openInputStream() throws IOException {
		return new ByteArrayInputStream(buffer, offset, size);
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		return get(buffer, this.offset + offset, Math.min(size - offset, length));
	}
	
	//// Normal stuff
	
	public int hashCode() {
		return BlobUtil.hashCode(buffer, offset, size);
	}
	
	public boolean equals( Object o ) {
		if( o instanceof ByteChunk ) return BlobUtil.equals( this, (ByteChunk)o );
		return false;
	}
	
	public String toString() {
		// TODO: when not valid UTF-8 text, do it different
		return BlobUtil.string(buffer, offset, size);
	}
}
