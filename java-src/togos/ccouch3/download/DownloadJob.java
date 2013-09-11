package togos.ccouch3.download;

import java.util.HashSet;

import togos.ccouch3.repo.Repository;

public class DownloadJob
{
	public final String urn;
	public final Repository localRepo;
	public boolean downloadCompleted;
	
	public DownloadJob( String urn, Repository localRepo ) {
		this.urn = urn;
		this.localRepo = localRepo;
	}
	
	public HashSet<DownloadRepo> reposToTry = new HashSet<DownloadRepo>();
}
