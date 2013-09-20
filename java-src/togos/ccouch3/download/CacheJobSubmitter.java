package togos.ccouch3.download;

import java.util.concurrent.ArrayBlockingQueue;

import togos.ccouch3.ActiveJobSet;

class CacheJobSubmitter {
	final ActiveJobSet<String> activeJobSet;
	final ArrayBlockingQueue<CacheJob> cacheJobQueue;
	
	public CacheJobSubmitter(
		ActiveJobSet<String> activeJobSet,
		ArrayBlockingQueue<CacheJob> cacheJobQueue
	) {
		this.activeJobSet = activeJobSet;
		this.cacheJobQueue = cacheJobQueue;
	}
	
	public void addUrn( String urn ) throws InterruptedException {
		if( activeJobSet.add(urn) ) {
			cacheJobQueue.put(new CacheJob(urn));
		}
	}
}