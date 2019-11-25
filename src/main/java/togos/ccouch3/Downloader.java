package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import togos.blob.ByteBlob;
import togos.ccouch3.Downloader.RepositorySet.RemoteRepository;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.repo.StoreException;
import togos.ccouch3.util.AddableSet;
import togos.ccouch3.util.EmptyAddableSet;
import togos.ccouch3.util.SLFStringSet;

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
		public RepositorySet( String[] repositoryUrls, int copies ) {
			this.repositories = new ArrayList<RemoteRepository>(copies*repositoryUrls.length);
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
	
	/**
	 * A queue that can be added to in either an immediate (via add)
	 * or blocking (via put) manner.
	 * Also you can take random items from it.
	 */
	static class BQ<Item> {
		private final ArrayList<Item> items = new ArrayList<Item>();
		private final int capacity;
		private final Random rand = new Random();
		
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
		public synchronized Item takeRandom() throws InterruptedException {
			while( items.size() == 0 ) wait();
			Item i = items.remove( rand.nextInt(items.size()) );
			notifyAll();
			return i;
		}
	}
	
	/**
	 * The set of all jobs that we want to do or are currently doing.
	 * When this becomes empty, we're done.
	 * I think that makes this a superset of urnQueue?
	 *   (Should have documented how this works when I wrote it; duh.)
	 */
	private final ActiveJobSet<String> enqueuedAndInProgressUrns = new ActiveJobSet<String>();
	/**
	 * Queue of jobs to be run.
	 */
	private final BQ<String> enqueuedUrns = new BQ<String>(32);
	
	final RepositorySet remoteRepoSet;
	final Repository localRepo;
	final AddableSet<String> fullyCachedUrns;
	final ArrayList<DownloadThread> downloadThreads;
	final AtomicInteger failCount = new AtomicInteger();
	
	/**
	 * The digestor to use to generate URNs when asked to cache something
	 * other than by its URN.
	 */
	public StreamURNifier digestor = BitprintDigest.STREAM_URNIFIER;
	
	/**
	 * User is doing something odd.
	 */
	public boolean reportWarnings = true;
	/**
	 * Errors other than 404s
	 */
	public boolean reportErrors = true;
	/**
	 * When a blob cannot be found anywhere
	 */
	public boolean reportFailures = true;
	/**
	 * Any connection error or error status other than a 404
	 */
	public boolean reportDownloadFailures = false;
	/**
	 * When starting a download
	 */
	public boolean reportDownloads = false;
	public boolean reportUnrecursableBlobs = false;
	/**
	 * Random diagnostic info that my be useful for debugging
	 */
	public boolean beChatty = false;
	
	public BlobReferenceScanMode scanMode;
	// AS7Q5NVWLNDPLRL3A7RYHPXY3QSMPQVI.3LPFRNN2WLY3WMKEHJ2Y3VYKEYMREOGZARVS4YY
	
	public Downloader( RepositorySet repoSet, Repository localRepo, AddableSet<String> fullyCachedUrns ) {
		this.remoteRepoSet = repoSet;
		this.localRepo = localRepo;
		this.fullyCachedUrns = fullyCachedUrns;
		this.downloadThreads = new ArrayList<DownloadThread>(remoteRepoSet.size());
		for( int i=repoSet.size()-1; i>=0; --i ) {
			downloadThreads.add(new DownloadThread("Download thread "+i));
		}
	}
	
	protected boolean isObviouslyFullyCached( String urn ) {
		return
			scanMode == BlobReferenceScanMode.NEVER ? localRepo.contains(urn) :
			fullyCachedUrns.contains(urn);
	}
	
	BlobReferenceScanner brs = null;
	protected BlobReferenceScanner getBlobReferenceScanner() {
		if( brs == null ) {
			brs = new BlobReferenceScanner(scanMode);
			brs.reportUnrecursableBlobs = reportUnrecursableBlobs;
			brs.reportErrors = reportErrors;
		}
		return brs;
	}
	
	/**
	 * Scan the blob identified by 'urn' for URNs, calling forEach for each one found.
	 * It will return true only if the blob identified by 'urn' is successfully found
	 * and scanned and forEach returns true for all embedded URNs.
	 * If shortCircuit is true, this will return as soon as forEach returns false.
	 */
	protected boolean scanForUrns( String urn, BlobReferenceScanner.ScanCallback forEach, boolean shortCircuit ) {
		InputStream is;
		
		try {
			is = localRepo.getInputStream(urn);
		} catch( IOException e ) {
			if( reportErrors ) {
				System.err.println("Error while scanning for URNs in "+urn+" due to an "+e.getClass().getName());
				e.printStackTrace();
			}
			return false;
		}
		
		if( is == null ) {
			if( reportUnrecursableBlobs ) {
				System.err.println("Blob not found in local repo; can't scan: "+urn);
			}
			return false;
		}
		
		return getBlobReferenceScanner().scanTextForUrns(urn, is, forEach, shortCircuit);
	}
	
	protected void cacheRecurse( String urn ) {
		if( scanMode == BlobReferenceScanMode.NEVER ) return;
		
		boolean fullyCached = scanForUrns(urn, new BlobReferenceScanner.ScanCallback() {
			@Override public boolean handle(String urn) {
				if( isObviouslyFullyCached(urn) ) return true;
				
				enqueueImmediately(urn);
				return false;
			}
		}, false);
		
		if( fullyCached ) fullyCachedUrns.add(urn);
	}
	
	/**
	 * Depth-first, short-circuiting, memoizing check.
	 * Using this on a completely cached tree will ensure that
	 * the tree is marked as fully cached.
	 * 
	 * First invocation:
	 *   isFullyCached() - check fully-cached-<scan method>.slf2;
	 *     it's not marked, and some blobs missing.
	 *   recurse()       - recursively download all blobs; leaves are marked fully cached
	 * Second invocation:
	 *   isFullyCached() - check fully-cached-<scan method>.slf2;
	 *     it's not marked, but all blobs are now present, so we mark it.
	 * Third invocation:
	 *   isFullyCached() - check fully-cached-<scan method>.slf2;
	 *     it is already marked, so we're done.
	 */
	protected boolean isFullyCached( String urn ) {
		if( isObviouslyFullyCached(urn) ) return true;
		
		if( localRepo.contains(urn) ) {
			boolean fullyCached = scanForUrns(urn, new BlobReferenceScanner.ScanCallback() {
				@Override public boolean handle(String v) {
					return isFullyCached(v);
				}
			}, true);
			if( fullyCached ) fullyCachedUrns.add(urn);
			return fullyCached;
		} else {
			return false;
		}
	}
	
	// At the expense of memory,
	// we can avoid enqueuing things more than once.
	protected HashSet<String> pushed = null;
	public void setRememberAttempts(boolean nf) {
		pushed = nf ? new HashSet<String>() : null;
	}
	protected boolean alreadyPushed(String urn) {
		if( pushed == null ) return false;
		synchronized(pushed) { return pushed.contains(urn); } 
	}
	protected void notePushed(String urn) {
		if( pushed == null ) return;
		synchronized(pushed) { pushed.add(urn); } 
	}
	
	class DownloadThread extends Thread {
		public DownloadThread(String name) {
			super(name);
		}
		
		protected boolean downloadFrom( String repoUrl, String urn )
			throws MalformedURLException
		{
			Exception connectionError = null;
			URL fullUrl = new URL(repoUrl+urn);
			try {
				URLConnection urlC = fullUrl.openConnection();
				urlC.connect();
				InputStream is = urlC.getInputStream();
				if( reportDownloads ) {
					System.err.println(this.getName()+" downloading "+fullUrl+" ("+urlC.getContentLength()+" bytes)");
				}
				localRepo.put(urn, is);
				if( reportDownloads ) {
					System.err.println(this.getName()+" completed download of "+fullUrl);
				}
				return true;
			} catch( FileNotFoundException e ) {
				// 404d!
			} catch( NoRouteToHostException e ) {
				connectionError = e;
			} catch( UnknownHostException e ) {
				connectionError = e;
			} catch( ConnectException e ) {
				connectionError = e;
			} catch( SocketException e ) {
				connectionError = e;
			} catch( IOException e ) {
				if( reportErrors ) {
					System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
				}
			} catch( StoreException e ) {
				if( reportErrors ) {
					System.err.println(e.getClass().getName()+" when downloading "+fullUrl+": "+e.getMessage());
				}
			}
			
			if( connectionError != null && reportDownloadFailures ) {
				System.err.println(connectionError.getClass().getName()+" when downloading "+fullUrl+": "+connectionError.getMessage());
			}
			
			return false;
		}
		
		protected boolean cache( String urn )
			throws InterruptedException, MalformedURLException
		{
			if( localRepo.contains(urn) ) return true;
			if( alreadyPushed(urn) ) return false;
			
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
			notePushed(urn);
			return false;
		}
				
		protected void dealWith( String urn ) {
			boolean success = false;
			try {
				if( (success = cache(urn)) ) cacheRecurse(urn);
			} catch( MalformedURLException e ) {
				e.printStackTrace();
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
			} finally {
				enqueuedAndInProgressUrns.remove(urn);
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
					if( beChatty ) System.err.println(this.getName()+" waiting for a job...");
					urn = enqueuedUrns.takeRandom();
					if( beChatty ) System.err.println(this.getName()+" going to try downloading "+urn);
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
	
	/**
	 * Checks if the specified URN is already
	 * enqueued, in progress, or completed.
	 * If not, marks it as enqueued and returns true to indicate 'go ahead and enqueue'.
	 * Otherwise returns false, indicating that there's nothing further to do.
	 */
	private boolean prequeue( String urn ) {
		assert urn.startsWith("urn:");
		assert !stopped;
		
		if( isFullyCached(urn) ) {
			if( beChatty ) System.err.println(urn+" already cached; not queing again");
			return false;
		}
		if( alreadyPushed(urn) ) {
			if( beChatty ) System.err.println(urn+" already queued; not queing again");
			return false;
		}
		
		if( downloadThreads.size() == 0 ) {
			failCount.incrementAndGet();
			if( reportFailures ) {
				System.err.println("No download threads; can't enqueue "+urn);
			}
			return false;
		}

		boolean added = enqueuedAndInProgressUrns.add(urn);
		if( beChatty ) {
			if( added ) System.err.println(urn+" added to set of queued or active jobs");
			else System.err.println(urn+" already in set of queued or active jobs; not re-queued");
		}
		return added;
	}
	
	public void enqueue( String urn ) throws InterruptedException {
		if( prequeue(urn) ) {
			if( beChatty ) System.err.println("Adding "+urn+" to job queue...");
			enqueuedUrns.put(urn);
			if( beChatty ) System.err.println("Added");
		}
	}
	
	public void enqueueImmediately( String urn ) {
		if( prequeue(urn) ) {
			if( beChatty ) System.err.println("Adding (nonblocking) "+urn+" queue");
			enqueuedUrns.add(urn);
		}
	}
	
	public void enqueueArg( String arg, BlobResolver rez )
		throws IOException, FileNotFoundException, InterruptedException
	{
		if( arg.startsWith("@") && arg.length() > 1 ) {
			final String listUrn = arg.substring(1);
			final ByteBlob listBlob;
			if( beChatty ) System.err.println("Opening list: "+listUrn);
			try {
				listBlob = rez.getBlob(listUrn);
			} catch( IOException e ) {
				failCount.incrementAndGet();
				if( reportFailures ) {
					System.err.println("Error finding "+listUrn+": "+e.getMessage());
				}
				return;
			}
			final BufferedReader listReader = new BufferedReader(new InputStreamReader(listBlob.openInputStream()));
			try {
				if( beChatty ) System.err.println("Reading list from "+listUrn+"...");
				String line;
				int lineCount = 0;
				while( (line = listReader.readLine()) != null ) {
					line = line.trim();
					if( line.startsWith("#") || line.isEmpty() ) continue;
					
					++lineCount;
					enqueue(line);
				}
				if( beChatty ) System.err.println("Read "+lineCount+" non-blank/comment lines from "+listUrn);
			} finally {
				listReader.close();
			}
		} else {
			String urn;
			if( arg.startsWith("urn:") ) {
				urn = arg;
			} else {
				ByteBlob blob;
				try {
					blob = rez.getBlob(arg);
				} catch( IOException e ) {
					failCount.incrementAndGet();
					if( reportFailures ) {
						System.err.println("Error finding "+arg+": "+e.getMessage());
					}
					return;
				}
				urn = digestor.digest(blob.openInputStream());
			}
			enqueue(urn);
		}
	}
	
	public synchronized void join() throws InterruptedException {
		assert started;
		assert !stopped;
		
		enqueuedAndInProgressUrns.waitUntilEmpty();
		
		for( DownloadThread dt : downloadThreads ) dt.interrupt();
		for( DownloadThread dt : downloadThreads ) dt.join();
		
		stopped = true;
	}

	protected static final String USAGE =
		"Usage: ccouch3 cache [<options>] {<urn>|@<urn-list-file>} ...\n" +
		"\n" +
		"Options:\n" +
		"  -repo <path>       ; path to local repository\n" +
		"  -remote-repo <url> ; URL of remote repository (slightly fuzzy; see notes)\n" +
		"  -recurse[:<mode>]  ; recursively scan cached blobs for new URNs to cache\n" +
		"  -recurse:?         ; dump recursion mode options\n" +
		"  -v                 ; be somewhat noisy\n" +
		"  -debug             ; be very noisy\n" +
		"  -silent            ; say nothing, ever\n" +
		"  -sector <name>     ; sector within local repo to store data in\n" +
		"  -remember-missing  ; avoid re-attempting failed fetches\n"+
		"  -connections-per-remote <n>\n" +
		"\n" +
		"URLs of remote repositories will be used as follows:\n" +
		"         hostname      -> http://hostname/uri-res/N2R?<qs-escaped-urn>\n" +
		"  http://hostname      -> http://hostname/uri-res/N2R?<qs-escaped-urn>\n" +
		"  http://hostname/...  -> http://hostname/...?<qs-escaped-urn>\n" +
		"  http://hostname/...? -> http://hostname/...?<qs-escaped-urn>\n" +
		"  http://hostname/.../ -> http://hostname/.../<pathseg-escaped-urn>";
	
	public static int main( Iterator<String> args )
		throws IOException, InterruptedException
	{
		int connectionsPerRemote = 2;
		BlobReferenceScanMode scanMode = BlobReferenceScanMode.NEVER;
		List<String> urnArgs = new ArrayList<String>();
		RepoConfig repoConfig = new RepoConfig();
		boolean reportDownloads = false;
		boolean reportUnrecursableBlobs = false;
		boolean reportDownloadFailures = false;
		boolean reportWarnings = true;
		boolean reportErrors = true;
		boolean reportFailures = true;
		boolean beChatty = false;
		boolean summarizeWhenCompletedWithFailures = true;
		// This is a very reasonable thing to do, but is
		// false by default because the set may take up a lot of memory.
		boolean rememberAttempts = false;
		
		while( args.hasNext() ) {
			String arg = args.next();
			BlobReferenceScanMode parsedScanMode;

			if( !arg.startsWith("-") ) {
				urnArgs.add(arg);
			} else if( (parsedScanMode = BlobReferenceScanMode.parseRecurseArg(arg)) != null ) {
				scanMode = parsedScanMode;
			} else if( "-silent".equals(arg) ) {
				reportWarnings = false;
				reportDownloads = false;
				reportErrors = false;
				reportFailures = false;
				summarizeWhenCompletedWithFailures = false;
				beChatty = false;
			} else if( "-v".equals(arg) ) {
				reportWarnings = true;
				reportErrors = true;
				reportFailures = true;
				reportUnrecursableBlobs = true;
				reportDownloadFailures = true;
			} else if( "-debug".equals(arg) ) {
				reportWarnings = true;
				reportErrors = true;
				reportFailures = true;
				reportDownloads = true;
				reportUnrecursableBlobs = true;
				reportDownloadFailures = true;
				beChatty = true;
			} else if( repoConfig.parseCommandLineArg(arg, args) ) {
			} else if( "-connections-per-remote".equals(arg) ) {
				connectionsPerRemote = Integer.parseInt(args.next());
			} else if( "-remember-missing".equals(arg) || "-remember-attempts".equals(arg) ) {
				rememberAttempts = true;
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE);
				return 0;
			} else {
				System.err.println("Error: unrecognized option: '"+arg+"'");
				System.err.println(USAGE);
				return 1;
			}
		}
		
		repoConfig.fix();
		final File primaryRepoDir = repoConfig.getPrimaryRepoDir();
		final SHA1FileRepository localRepo = repoConfig.getPrimaryRepository();
		
		final AddableSet<String> fullyCachedTreeUrns =
			scanMode == BlobReferenceScanMode.NEVER ? EmptyAddableSet.<String>getInstance() :
			new SLFStringSet(new File(primaryRepoDir, "cache/ccouch3-downloader/fully-cached-"+scanMode.cacheDbName.toLowerCase()+".slf2"));
		
		final BlobResolver argBlobResolver = CCouch3Command.getCommandLineFileResolver(new File[]{primaryRepoDir});
		
		if( repoConfig.getRemoteRepoUrls().length == 0 && reportWarnings ) {
			System.err.println("Warning: No remote reposisories listed; no downloads will succeed");
		}
		
		Downloader downloader = new Downloader( new RepositorySet(repoConfig.getRemoteRepoUrls(), connectionsPerRemote), localRepo, fullyCachedTreeUrns );
		downloader.scanMode = scanMode;
		downloader.reportDownloads = reportDownloads;
		downloader.reportUnrecursableBlobs = reportUnrecursableBlobs;
		downloader.reportDownloadFailures = reportDownloadFailures;
		downloader.reportWarnings = reportWarnings;
		downloader.reportErrors = reportErrors;
		downloader.reportFailures = reportFailures;
		downloader.beChatty = beChatty;
		downloader.setRememberAttempts(rememberAttempts);
		
		downloader.start();
		try {
			for( String urnArg : urnArgs ) {
				downloader.enqueueArg( urnArg, argBlobResolver );
			}
		} finally {
			downloader.join();
		}
		
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
