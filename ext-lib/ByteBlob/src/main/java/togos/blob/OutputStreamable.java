package togos.blob;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputStreamable
{
	public void writeTo(OutputStream os) throws IOException;
}
