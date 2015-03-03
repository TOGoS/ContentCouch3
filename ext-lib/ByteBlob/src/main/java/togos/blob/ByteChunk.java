package togos.blob;

public interface ByteChunk extends ByteBlob
{

	public int getOffset();
	public long getSize();
	public byte[] getBuffer();
}
