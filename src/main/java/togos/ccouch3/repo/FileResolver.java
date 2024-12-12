package togos.ccouch3.repo;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface FileResolver
{
	/**
	 * Return a file given a name.
	 * Should never return null.
	 * May throw a IOException to indicate that the file could not be found for some reason
	 * (FileNotFoundException is recommended for generic 'I don't have that' errors).   
	 */
	public File getFile(String name) throws IOException;
	
	/**
	 * Return a list of all files for the given name.
	 * Note that resources may be stored in ways that do
	 * not directly map to files, and those cases won't
	 * be accounted for in the resulting list.
	 * 
	 * This should always return a list.
	 * The list may be empty.
	 */
	public List<File> getFiles(String name);
}
