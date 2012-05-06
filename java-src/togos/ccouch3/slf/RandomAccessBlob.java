package togos.ccouch3.slf;

import java.io.Flushable;

import togos.blob.ByteChunk;

public interface RandomAccessBlob extends Flushable
{
	public long getSize();
	public ByteChunk get( long offset, int length );
	public void put( long offset, ByteChunk data );
}
