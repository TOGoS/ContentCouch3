package togos.blob;

public interface ByteBlob extends InputStreamable
{
	public long getSize();
	public ByteBlob slice(long offset, long length);
}
