package togos.ccouch3.repo;

import java.io.File;
import java.io.FileNotFoundException;

public interface FileResolver
{
	public File getFile(String name) throws FileNotFoundException;
}
