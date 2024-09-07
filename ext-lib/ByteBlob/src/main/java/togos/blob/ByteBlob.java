package togos.blob;

public interface ByteBlob extends InputStreamable, OutputStreamable
{
	public long getSize();
	public ByteBlob slice(long offset, long length);
}
