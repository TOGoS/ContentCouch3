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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	static final Charset UTF8 = Charset.forName("UTF-8");
	static final Pattern SHA1_OR_BITPRINT_PATTERN = Pattern.compile(
		"urn:(sha1:[A-Z0-9]{32}|bitprint:[A-Z0-9]{32}\\.[A-Z0-9]{39})",
		Pattern.CASE_INSENSITIVE);
	
	final ActiveJobSet<String> enqueuedUrns = new ActiveJobSet<String>();
	
	private final BQ<String> urnQueue = new BQ<String>(10);
	
	final RepositorySet remoteRepoSet;
	final Repository localRepo;
	final ArrayList<DownloadThread> downloadThreads;
	final AtomicInteger failCount = new AtomicInteger();
	
	/**
	 * Errors other than 404s
	 */
	public boolean reportErrors = true;
	/**
	 * When a blob cannot be found anywhere
	 */
	public boolean reportFailures = true;
	/**
	 * When starting a download
	 */
	public boolean reportDownloads = false;
	public boolean reportUnrecursableBlobs = false;
	
	static enum RecursionMode { NEVER, TEXT }
	
	public RecursionMode recursionMode;
	public int recursionSizeLimit;
	// AS7Q5NVWLNDPLRL3A7RYHPXY3QSMPQVI.3LPFRNN2WLY3WMKEHJ2Y3VYKEYMREOGZARVS4YY
	public Pattern urnPattern = SHA1_OR_BITPRINT_PATTERN;
	
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
				if( reportDownloads ) {
					System.err.println("Downloading "+fullUrl+" ("+urlC.getContentLength()+" bytes)");
				}
				localRepo.put(urn, is);
				if( reportDownloads ) {
					System.err.println("Completed download of "+fullUrl);
				}
				return true;
			} catch( FileNotFoundException e ) {
				// 404d!
			} catch( IOException e ) {
				if( reportErrors ) {
					System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
				}
			} catch( StoreException e ) {
				if( reportErrors ) {
					System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
				}
			}
			return false;
		}
		
		protected boolean download( String urn )
			throws InterruptedException, MalformedURLException
		{
			if( localRepo.contains(urn) ) {
				// System.err.println(urn + " already exists locally");
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
		
		protected void recurse( String urn ) {
			if( recursionMode == RecursionMode.NEVER ) return;
			
			try {
				InputStream is = localRepo.getInputStream(urn);
				if( is == null ) {
					if( reportUnrecursableBlobs ) {
						System.err.println("Blob not found in local repo; can't recurse: "+urn);
					}
					return;
				}
				
				CharsetDecoder utf8decoder = UTF8.newDecoder();
				// Have it throw CharacterCodingException on non-UTF-8 input! 
				utf8decoder.onMalformedInput(CodingErrorAction.REPORT);
				
				BufferedReader br = new BufferedReader(new InputStreamReader(is, utf8decoder));
				String line;
				Matcher matcher = urnPattern.matcher("");
				while( (line = br.readLine()) != null ) {
					matcher.reset(line);
					while( matcher.find() ) {
						enqueueImmediately(matcher.group());
					}
				}
			} catch( CharacterCodingException e ) {
				if( reportUnrecursableBlobs ) {
					System.err.println("Not valid UTF-8; can't recurse: "+urn);
				}
			} catch( IOException e ) {
				if( reportErrors ) {
					System.err.println("Could not recurse on "+urn+" due to an "+e.getClass().getName());
					e.printStackTrace();
				}
			}
		}
		
		protected void dealWith( String urn ) {
			boolean success = false;
			try {
				success = download(urn);
				
				if( success ) recurse(urn);
			} catch( MalformedURLException e ) {
				e.printStackTrace();
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
			} finally {
				enqueuedUrns.remove(urn);
				if( !success ) {
					failCount.incrementAndGet();
					if( reportFailures ) {
						System.err.println("Couldn't find "+urn);
					}
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
	
	public void enqueueImmediately( String urn ) {
		assert !stopped;
		
		if( enqueuedUrns.add(urn) ) {
			urnQueue.add(urn);
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
		RecursionMode recursionMode = RecursionMode.NEVER;
		List<String> urnArgs = new ArrayList<String>();
		List<String> remoteRepoUrls = new ArrayList<String>();
		boolean reportDownloads = false;
		boolean reportUnrecursableBlobs = false;
		boolean reportErrors = true;
		boolean reportFailures = true;
		boolean summarizeWhenCompletedWithFailures = true;
		
		while( args.hasNext() ) {
			String arg = args.next();
			if( !arg.startsWith("-") ) {
				urnArgs.add(arg);
			} else if( "-recurse".equals(arg) ) {
				recursionMode = RecursionMode.TEXT;
			} else if( "-debug".equals(arg) ) {
				reportDownloads = true;
				reportUnrecursableBlobs = true;
			} else if( "-silent".equals(arg) ) {
				reportDownloads = false;
				reportErrors = false;
				reportFailures = false;
				summarizeWhenCompletedWithFailures = false;
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
		downloader.recursionMode = recursionMode;
		downloader.reportDownloads = reportDownloads;
		downloader.reportUnrecursableBlobs = reportUnrecursableBlobs;
		downloader.reportErrors = reportErrors;
		downloader.reportFailures = reportFailures;
		
		downloader.start();
		for( String urnArg : urnArgs ) downloader.enqueueArg( urnArg, System.in );
		downloader.join();
		
		int failures = downloader.failCount.intValue();
		if( failures > 0 ) {
			if( summarizeWhenCompletedWithFailures ) {
				String noun = failures > 1 ? "resources" : "resource";
				System.err.println(failures + " " + noun + " could not be cached.");
			}
			return 2;
		} else {
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( main( Arrays.asList(args).iterator()) );
	}
}
