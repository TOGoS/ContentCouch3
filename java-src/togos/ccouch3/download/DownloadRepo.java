package togos.ccouch3.download;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ArrayBlockingQueue;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.StoreException;

class DownloadRepo
{
	static class DownloadThread extends Thread {
		final DownloadRepo sourceRepository;
		final Repository destRepository;
		
		public DownloadThread( DownloadRepo src, Repository dest ) {
			super(src.urlPrefix + " download thread");
			this.sourceRepository = src;
			this.destRepository = dest;
		}
		
		public void _run()
			throws InterruptedException, MalformedURLException, IOException, StoreException
		{
			while(true) {
				DownloadJob job = sourceRepository.inputQueue.take();
				
				InputStream is;
				try {
					URLConnection urlConn = new URL(sourceRepository.urlPrefix + job.urn).openConnection();
					is = urlConn.getInputStream();
				} catch( FileNotFoundException e ) {
					is = null;
				}
				
				if( is != null ) {
					destRepository.put(job.urn, is);
					job.completedSuccessfully = true;
				}
				
				sourceRepository.returnQueue.put(job);
			}
		}
		
		public void run() {
			try {
				_run();
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			} catch( Exception e ) {
				throw new RuntimeException(e);
			}
		}
	};
	
	public final String urlPrefix;
	private int capacity;
	
	private final ArrayBlockingQueue<DownloadJob> inputQueue, returnQueue;
	private DownloadThread[] downloadThreads;
	
	/**
	 * @param urlPrefix  e.g. "http://131.172.168.104/uri-res/"
	 * @param capacity number of requests to allow at once; usually 2
	 */
	public DownloadRepo( String urlPrefix, int capacity, Repository localRepo, ArrayBlockingQueue<DownloadJob> returnQueue ) {
		this.urlPrefix = urlPrefix;
		this.capacity = capacity;
		this.inputQueue = new ArrayBlockingQueue<DownloadJob>(capacity);
		this.returnQueue = returnQueue;
		this.downloadThreads = new DownloadThread[capacity];
		for( int i=0; i<capacity; ++i ) {
			downloadThreads[i] = new DownloadThread(this, localRepo);
		}
	}
	
	public boolean accept( DownloadJob j ) {
		synchronized( this ) {
			if( capacity == 0 ) return false;
			--capacity;
		}
		
		j.reposToTry.remove(this);
		inputQueue.add(j);
		return true;
	}
	
	public void start() {
		for( int i=0; i<capacity; ++i ) {
			downloadThreads[i].start();
		}
	}
}
