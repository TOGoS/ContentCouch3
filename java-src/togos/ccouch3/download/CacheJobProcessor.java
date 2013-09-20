package togos.ccouch3.download;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import togos.ccouch3.ActiveJobSet;
import togos.ccouch3.repo.Repository;

class CacheJobProcessor extends Processor
{
	final BlockingQueue<CacheJob> cacheJobSource;
	final Repository localRepository;
	final ActiveJobSet<String> activeJobSet;
	final Collection<DownloadRepo> downloadRepos;
	final BlockingQueue<DownloadJob> downloadJobSink;
	
	public CacheJobProcessor(
		BlockingQueue<CacheJob> cacheJobSource,
		Repository localRepository,
		ActiveJobSet<String> activeJobSet,
		Collection<DownloadRepo> downloadRepos,
		BlockingQueue<DownloadJob> downloadJobSink
	) {
		this.cacheJobSource = cacheJobSource;
		this.localRepository = localRepository;
		this.activeJobSet = activeJobSet;
		this.downloadRepos = downloadRepos;
		this.downloadJobSink = downloadJobSink;
	}
	
	@Override protected void step()
		throws InterruptedException
	{
		CacheJob cj = cacheJobSource.take();
		
		// TODO: if it's a recursive job,
		// might need to process chrilden, anyway.
		if( localRepository.contains(cj.urn) ) {
			activeJobSet.remove(cj.urn);
			return;
		}
		
		downloadJobSink.put(new DownloadJob(cj.urn, downloadRepos));
	}
}
