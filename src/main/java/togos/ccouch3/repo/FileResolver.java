package togos.ccouch3.repo;

import java.io.File;
import java.io.IOException;

public interface FileResolver
{
	/**
	 * Return a file given a name.
	 * Should never return null.
	 * May throw a IOException to indicate that the file could not be found for some reason
	 * (FileNotFoundException is recommended for generic 'I don't have that' errors).   
	 */
	public File getFile(String name) throws IOException;
}
