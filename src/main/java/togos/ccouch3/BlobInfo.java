package togos.ccouch3;

import java.io.IOException;
import java.io.InputStream;

public interface BlobInfo
{
	public String getUrn();
	public long getSize();
	public InputStream openInputStream() throws IOException;
}
