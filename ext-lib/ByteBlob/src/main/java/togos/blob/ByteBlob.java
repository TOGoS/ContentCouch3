package togos.blob;

import togos.blob.InputStreamable;

public interface ByteBlob extends InputStreamable
{
	public long getSize();
	public ByteBlob slice(long offset, long length);
}
