package togos.ccouch3.util;

import java.io.IOException;
import java.io.InputStream;

public class SubInputStream extends InputStream
{
	protected final InputStream backingStream;
	protected final long offset;
	protected final long length;
	protected long readFromBackingStream = 0;
	public SubInputStream( InputStream backingStream, long offset, long length ) {
		this.backingStream = backingStream;
		this.offset = offset;
		this.length = length;
	}
	
	@Override public void close() throws IOException {
		backingStream.close();
	}
	
	protected void skipToOffset() throws IOException {
		while( readFromBackingStream < offset ) {
			long skipped = backingStream.skip(offset - readFromBackingStream);
			if( skipped <= 0 ) return;
			readFromBackingStream += skipped;
		}
	}
	
	@Override public int read() throws IOException {
		skipToOffset();
		if( readFromBackingStream + offset >= length ) return -1;
		int read = backingStream.read();
		if( read != -1 ) ++readFromBackingStream;
		return read;
	}
	
	@Override public int read(byte[] b, int off, int len) throws IOException {
		skipToOffset();
		if( len + readFromBackingStream > offset + length ) {
			len = (int)(offset + length - readFromBackingStream);
		}
		int read = backingStream.read(b, off, len);
		readFromBackingStream += read;
		return read;
	}
	
	@Override public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override public long skip(long n) throws IOException {
		skipToOffset();
		long skipped = backingStream.skip(n);
		readFromBackingStream += skipped;
		return skipped;
	}
}
