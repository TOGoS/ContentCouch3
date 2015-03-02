package togos.blob;

import java.io.IOException;
import java.io.InputStream;

public interface InputStreamable
{
	public InputStream openInputStream() throws IOException;
}
