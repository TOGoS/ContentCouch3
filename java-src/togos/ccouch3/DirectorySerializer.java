package togos.ccouch3;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public interface DirectorySerializer
{
	public void serialize( Collection<DirectoryEntry> entries, OutputStream os ) throws IOException;
}
