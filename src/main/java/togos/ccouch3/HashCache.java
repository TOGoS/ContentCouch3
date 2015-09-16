package togos.ccouch3;

import java.io.File;

public interface HashCache
{
	public String getFileUrn( File f ) throws Exception;
	public void cacheFileUrn( File f, String urn ) throws Exception;
}
