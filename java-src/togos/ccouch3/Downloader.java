package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import togos.ccouch3.Downloader.RepositorySet.RemoteRepository;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.repo.StoreException;

public class Downloader
{
	static class RepositorySet {
		static class RemoteRepository {
			public final String url;
			protected boolean locked;
			
			public RemoteRepository( String url ) {
				this.url = url;
			}
		}
		
		private final List<RemoteRepository> repositories;
		public RepositorySet( Collection<String> repositoryUrls, int copies ) {
			this.repositories = new ArrayList<RemoteRepository>(copies*repositoryUrls.size());
			for( int i=0; i<copies; ++i ) {
				for( String url : repositoryUrls ) {
					repositories.add(new RemoteRepository(url));
				}
			}
		}
		public RepositorySet( Collection<RemoteRepository> repositories ) {
			this.repositories = new ArrayList<RemoteRepository>(repositories);
		}
		
		public RemoteRepository lockRepo( Set<String> exclude ) throws InterruptedException {
			Set<RemoteRepository> unexcluded = new HashSet<RemoteRepository>();
			
			for( RemoteRepository repo : repositories ) {
				if( !exclude.contains(repo.url) ) {
					unexcluded.add(repo);
				}
			}
			
			if( unexcluded.size() == 0 ) return null;
			
			synchronized(this) {
				while( true ) {
					for( RemoteRepository repo : unexcluded ) {
						if( !repo.locked ) {
							repo.locked = true;
							return repo;
						}
					}
					this.wait();
				}
			}
		}
		
		public synchronized void releaseRepo( RemoteRepository repo ) {
			repo.locked = false;
			this.notifyAll();
		}
		
		public int size() {
			return repositories.size();
		}
	}
	
	static class BQ<Item> {
		private final LinkedList<Item> items = new LinkedList<Item>();
		private final int capacity;
		
		public BQ( int capacity ) {
			this.capacity = capacity;
		}
		
		/** Add the item and return immediately */
		public synchronized void add( Item item ) {
			items.add(item);
			notifyAll();
		}
		/** Wait until there is free space and add the item */
		public synchronized void put( Item item ) throws InterruptedException {
			while( items.size() >= capacity ) wait();
			items.add(item);
			notifyAll();
		}
		public synchronized Item take() throws InterruptedException {
			while( items.size() == 0 ) wait();
			Item i = items.remove(0);
			notifyAll();
			return i;
		}
	}
	
	final ActiveJobSet<String> enqueuedUrns = new ActiveJobSet<String>();
	//final ActiveJobSet<String> downloadingUrns = new ActiveJobSet<String>();  
	
	private final BQ<String> urnQueue = new BQ<String>(10);
	
	final RepositorySet remoteRepoSet;
	final Repository localRepo;
	final ArrayList<DownloadThread> downloadThreads;
	final AtomicInteger failCount = new AtomicInteger();
	
	/**
	 * Errors other than 404s
	 */
	public boolean reportErrors;
	/**
	 * When a blob cannot be found anywhere
	 */
	public boolean reportFailures;
	/**
	 * When starting a download
	 */
	public boolean reportDownloads;
	
	public Downloader( RepositorySet repoSet, Repository localRepo ) {
		this.remoteRepoSet = repoSet;
		this.localRepo = localRepo;
		this.downloadThreads = new ArrayList<DownloadThread>(remoteRepoSet.size());
		for( int i=repoSet.size()-1; i>=0; --i ) {
			downloadThreads.add(new DownloadThread());
		}
	}
	
	class DownloadThread extends Thread {
		protected boolean downloadFrom( String repoUrl, String urn )
			throws MalformedURLException
		{
			URL fullUrl = new URL(repoUrl+urn);
			try {
				URLConnection urlC = fullUrl.openConnection();
				urlC.connect();
				InputStream is = urlC.getInputStream();
				System.err.println("Downloading "+fullUrl+" ("+urlC.getContentLength()+" bytes)");
				localRepo.put(urn, is);
				return true;
			} catch( FileNotFoundException e ) {
				// 404d!
			} catch( IOException e ) {
				System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
			} catch( StoreException e ) {
				System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
			}
			return false;
		}
		
		protected boolean download( String urn )
			throws InterruptedException, MalformedURLException
		{
			if( localRepo.contains(urn) ) {
				System.err.println(urn + " already exists locally");
				return true;
			}
			
			HashSet<String> failedRepos = new HashSet<String>();
			RemoteRepository repo;
			while( (repo = remoteRepoSet.lockRepo(failedRepos)) != null ) {
				try {
					if( downloadFrom( repo.url, urn ) ) return true;
				} finally {
					remoteRepoSet.releaseRepo(repo);
				}
				failedRepos.add(repo.url);
			}
			return false;
		}
		
		protected void dealWith( String urn ) {
			/*
			if( !downloadingUrns.add(urn) ) {
				// Someone's already dealing with it!
				return;
			}
			*/
			
			boolean success = false;
			try {
				success = download(urn);
				// TODO: if recursing, recurse
			} catch( MalformedURLException e ) {
				e.printStackTrace();
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
			} finally {
				//downloadingUrns.remove(urn);
				enqueuedUrns.remove(urn);
				if( !success ) {
					failCount.incrementAndGet();
					System.err.println("Couldn't find "+urn);
				}
			}
		}
		
		@Override public void run() {
			while(!interrupted()) {
				String urn;
				try {
					urn = urnQueue.take();
				} catch( InterruptedException e ) {
					Thread.currentThread().interrupt();
					continue;
				}
				
				dealWith(urn);
			}
		}
	}
	
	private boolean started, stopped;
	
	public synchronized void start() {
		assert !started;
		
		for( DownloadThread dt : downloadThreads ) dt.start();
		
		started = true;
	}
	
	public void enqueue( String urn ) throws InterruptedException {
		assert !stopped;
		
		if( enqueuedUrns.add(urn) ) {
			urnQueue.put(urn);
		}
	}
	
	public void enqueueArg( String arg, InputStream stdin )
		throws IOException, FileNotFoundException, InterruptedException
	{
		if( arg.startsWith("@") && arg.length() > 1 ) {
			final boolean closeOnEnd;
			final BufferedReader stream;
			
			if( "@-".equals(arg) ) {
				stream = new BufferedReader(new InputStreamReader(stdin));
				closeOnEnd = false;
			} else {
				String filename = arg.substring(1);
				// TODO: handle URIs, too
				stream = new BufferedReader(new FileReader(filename));
				closeOnEnd = true;
			}
			
			String line;
			while( (line = stream.readLine()) != null ) {
				line = line.trim();
				if( line.startsWith("#") ) continue;
				
				enqueue(line);
			}
			
			if( closeOnEnd ) stream.close();
		} else {
			enqueue(arg);
		}
	}
	
	public synchronized void join() throws InterruptedException {
		assert started;
		assert !stopped;
		
		enqueuedUrns.waitUntilEmpty();
		
		for( DownloadThread dt : downloadThreads ) dt.interrupt();
		for( DownloadThread dt : downloadThreads ) dt.join();
		
		stopped = true;
	}
	
	public static int main( Iterator<String> args )
		throws IOException, InterruptedException
	{
		String localRepoPath = "~/.ccouch";
		String cacheSector = "remote";
		int connectionsPerRemote = 2;
		List<String> urnArgs = new ArrayList<String>();
		List<String> remoteRepoUrls = new ArrayList<String>();
		
		while( args.hasNext() ) {
			String arg = args.next();
			if( !arg.startsWith("-") ) {
				urnArgs.add(arg);
			} else if( "-repo".equals(arg) ) {
				localRepoPath = args.next();
			} else if( "-remote-repo".equals(arg) ) {
				remoteRepoUrls.add(args.next());
			} else if( "-connections-per-remote".equals(arg) ) {
				connectionsPerRemote = Integer.parseInt(args.next());
			} else if( "-sector".equals(arg) ) {
				cacheSector = args.next();
			} else {
				System.err.println("Error: unrecognized option: '"+arg+"'");
				return 1;
			}
		}
		
		if( remoteRepoUrls.size() == 0 ) {
			System.err.println("Error: No remote repositories configured");
			return 1;
		}
		
		final File localRepoDir;
		if( localRepoPath.startsWith("~/") ) {
			localRepoDir = new File(System.getProperty("user.home"), localRepoPath.substring(2));
		} else {
			localRepoDir = new File(localRepoPath);
		}
		
		final SHA1FileRepository localRepo = new SHA1FileRepository(new File(localRepoDir, "data"), cacheSector);
		
		Downloader downloader = new Downloader( new RepositorySet(remoteRepoUrls, connectionsPerRemote), localRepo );
		downloader.start();
		for( String urnArg : urnArgs ) downloader.enqueueArg( urnArg, System.in );
		downloader.join();
		
		int failures = downloader.failCount.intValue();
		if( failures > 0 ) {
			String noun = failures > 1 ? "resources" : "resource";
			System.err.println(failures + " " + noun + " could not be cached.");
			return 2;
		} else {
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
