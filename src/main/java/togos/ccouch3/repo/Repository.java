package togos.ccouch3.repo;

import java.io.IOException;
import java.io.InputStream;

import togos.blob.ByteChunk;

/**
 * Note that getFile may return null without implying that the named blob does not exist in the repository.
 * It might just not be backed by a File.
 */
public interface Repository extends FileResolver
{
	public boolean contains( String urn );
	/**
	 * Attempt to read the data from the given input stream and write it named with the given URN.
	 * This method should *always* call 'close' on the InputStream before returning,
	 * even if there is an exception.
	 */
	public void put( String urn, InputStream is ) throws StoreException;
	
	public ByteChunk getChunk( String urn, int maxSize );
	
	public InputStream getInputStream( String urn ) throws IOException;
}
