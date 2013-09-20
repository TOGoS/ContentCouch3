package togos.ccouch3.download;

import java.util.concurrent.BlockingQueue;

import togos.ccouch3.repo.Repository;

class DownloadJobProcessor extends Processor
{
	final BlockingQueue<DownloadJob> jobSource;
	final BlockingQueue<DownloadJob> jobSink;
	
	public DownloadJobProcessor(
		BlockingQueue<DownloadJob> jobSource,
		BlockingQueue<DownloadJob> jobSink
	) {
		this.jobSource = jobSource;
		this.jobSink = jobSink;
	}
	
	@Override protected void step() throws InterruptedException {
		DownloadJob j = jobSource.take();
		if( j.completedSuccessfully || j.reposToTry.size() == 0 ) {
			// Done with this one!
			jobSink.put(j);
			return;
		}
		
		do {
			for( DownloadRepo repo : j.reposToTry ) {
				if( repo.accept(j) ) {
					return;
				}
			}
			// If we get here, we couldn't find any repos to take our job!
			// Crappy solution: wait a little while and try again,
			// preferrably with a different job, which may have additional
			// repos that it can be enqueued on
			Thread.sleep(500);
		} while( !jobSource.offer(j) );
	}
}
