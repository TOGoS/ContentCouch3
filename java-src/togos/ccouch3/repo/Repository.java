package togos.ccouch3.repo;

import java.io.InputStream;


public interface Repository
{
	public boolean contains( String urn );
	public void put( String urn, InputStream is ) throws StoreException;
}
