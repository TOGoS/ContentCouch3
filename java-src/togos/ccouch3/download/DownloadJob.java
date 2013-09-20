package togos.ccouch3.download;

import java.util.Collection;
import java.util.HashSet;

public class DownloadJob
{
	public final String urn;
	public boolean completedSuccessfully = false;
	public final HashSet<DownloadRepo> reposToTry;
	
	public DownloadJob( String urn, Collection<DownloadRepo> reposToTry ) {
		this.urn = urn;
		this.reposToTry = new HashSet<DownloadRepo>(reposToTry);
	}
}
