package togos.blob;

public interface ByteChunk extends ByteBlob
{
	public int getOffset();
	/**
	 * Returns a long for compatibility with {@link ByteBlob#getSize()}
	 * 
	 * Otherwise it would be silly, since by definition byte chunks are backed by arrays
	 * and length can't be longer than Integer.MAX_INT.
	 */
	public long getSize();
	public byte[] getBuffer();
}
