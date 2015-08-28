package togos.blob;

import togos.blob.InputStreamable;

public interface ByteBlob extends InputStreamable, OutputStreamable
{
	public long getSize();
	public ByteBlob slice(long offset, long length);
}
