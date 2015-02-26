package togos.ccouch3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import togos.ccouch3.Downloader.RepositorySet.RemoteRepository;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.repo.StoreException;
import togos.ccouch3.util.AddableSet;
import togos.ccouch3.util.EmptyAddableSet;
import togos.ccouch3.util.RepoURLDefuzzer;
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
	
	interface ScanCallback {
		public boolean handle(String t);
	}
	
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
	
	static final Charset UTF8 = Charset.forName("UTF-8");
	static final Pattern SHA1_OR_BITPRINT_PATTERN = Pattern.compile(
		"urn:(sha1:[A-Z0-9]{32}|bitprint:[A-Z0-9]{32}\\.[A-Z0-9]{39})",
		Pattern.CASE_INSENSITIVE);
	
	final ActiveJobSet<String> enqueuedUrns = new ActiveJobSet<String>();
	
	private final BQ<String> urnQueue = new BQ<String>(32);
	
	final RepositorySet remoteRepoSet;
	final Repository localRepo;
	final AddableSet<String> fullyCachedUrns;
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
	 * Any connection error or error status other than a 404
	 */
	public boolean reportDownloadFailures = false;
	/**
	 * When starting a download
	 */
	public boolean reportDownloads = false;
	public boolean reportUnrecursableBlobs = false;
	
	public BlobReferenceScanMode scanMode;
	// AS7Q5NVWLNDPLRL3A7RYHPXY3QSMPQVI.3LPFRNN2WLY3WMKEHJ2Y3VYKEYMREOGZARVS4YY
	public Pattern urnPattern = SHA1_OR_BITPRINT_PATTERN;
	
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
	
	/**
	 * Scan the blob identified by 'urn' for URNs, calling forEach for each one found.
	 * It will return true only if the blob identified by 'urn' is successfully found
	 * and scanned and forEach returns true for all embedded URNs.
	 * If shortCircuit is true, this will return as soon as forEach returns false.
	 */
	protected boolean scanForUrns( String urn, ScanCallback forEach, boolean shortCircuit ) {
		boolean success = true;
		InputStream is = null;
		try {
			is = localRepo.getInputStream(urn);
			if( is == null ) {
				if( reportUnrecursableBlobs ) {
					System.err.println("Blob not found in local repo; can't scan: "+urn);
				}
				return false;
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
					if( !forEach.handle(matcher.group()) ) {
						success = false;
						if( shortCircuit ) return false;
					}
				}
			}
		} catch( CharacterCodingException e ) {
			// This is normal and counts as success!
			if( reportUnrecursableBlobs ) {
				System.err.println("Not valid UTF-8; can't scan: "+urn);
			}
		} catch( IOException e ) {
			// This is not, and doesn't.
			success = false;
			if( reportErrors ) {
				System.err.println("Error while scanning for URNs in "+urn+" due to an "+e.getClass().getName());
				e.printStackTrace();
			}
		} finally {
			if( is != null ) try {
				is.close();
			} catch( IOException e ) {
				if( reportErrors ) {
					System.err.println("Failed to close InputStream of "+urn+" after scanning for URNs");
				}
			}
		}
		return success;
	}
	
	protected void cacheRecurse( String urn ) {
		if( scanMode == BlobReferenceScanMode.NEVER ) return;
		
		boolean fullyCached = scanForUrns(urn, new ScanCallback() {
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
			boolean fullyCached = scanForUrns(urn, new ScanCallback() {
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
					System.err.println("Downloading "+fullUrl+" ("+urlC.getContentLength()+" bytes)");
				}
				localRepo.put(urn, is);
				if( reportDownloads ) {
					System.err.println("Completed download of "+fullUrl);
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
					urn = urnQueue.takeRandom();
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
	
	protected boolean prequeue( String urn ) {
		assert !stopped;
		
		if( isFullyCached(urn) ) return false;
		if( alreadyPushed(urn) ) return false;
		
		return enqueuedUrns.add(urn);
	}
	
	public void enqueue( String urn ) throws InterruptedException {
		if( prequeue(urn) ) urnQueue.put(urn);
	}
	
	public void enqueueImmediately( String urn ) {
		if( prequeue(urn) ) urnQueue.add(urn);
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

	protected static final String USAGE =
		"Usage: ccouch3 cache [<options>] {<urn>|@<urn-list-file>} ...\n" +
		"\n" +
		"Options:\n" +
		"  -repo <path>       ; path to local repository\n" +
		"  -remote-repo <url> ; URL of remote repository (slightly fuzzy; see notes)\n" +
		"  -recurse           ; recursively scan cached blobs for new URNs to cache\n" +
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
		String localRepoPath = "~/.ccouch";
		String cacheSector = "remote";
		int connectionsPerRemote = 2;
		BlobReferenceScanMode scanMode = BlobReferenceScanMode.NEVER;
		List<String> urnArgs = new ArrayList<String>();
		List<String> remoteRepoUrls = new ArrayList<String>();
		boolean reportDownloads = false;
		boolean reportUnrecursableBlobs = false;
		boolean reportDownloadFailures = false;
		boolean reportErrors = true;
		boolean reportFailures = true;
		boolean summarizeWhenCompletedWithFailures = true;
		// This is a very reasonable thing to do, but is
		// false by default because the set may take up a lot of memory.
		boolean rememberAttempts = false;
		
		while( args.hasNext() ) {
			String arg = args.next();
			if( !arg.startsWith("-") ) {
				urnArgs.add(arg);
			} else if( "-recurse".equals(arg) ) {
				scanMode = BlobReferenceScanMode.TEXT;
			} else if( "-debug".equals(arg) ) {
				reportDownloads = true;
				reportUnrecursableBlobs = true;
				reportDownloadFailures = true;
			} else if( "-silent".equals(arg) ) {
				reportDownloads = false;
				reportErrors = false;
				reportFailures = false;
				summarizeWhenCompletedWithFailures = false;
			} else if( "-repo".equals(arg) ) {
				localRepoPath = args.next();
			} else if( "-remote-repo".equals(arg) ) {
				remoteRepoUrls.add(RepoURLDefuzzer.defuzzRemoteRepoPrefix(args.next()));
			} else if( "-connections-per-remote".equals(arg) ) {
				connectionsPerRemote = Integer.parseInt(args.next());
			} else if( "-sector".equals(arg) ) {
				cacheSector = args.next();
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
		
		final AddableSet<String> fullyCachedTreeUrns =
			scanMode == BlobReferenceScanMode.NEVER ? EmptyAddableSet.<String>getInstance() :
			new SLFStringSet(new File(localRepoDir, "cache/ccouch3-downloader/fully-cached-"+scanMode.name().toLowerCase()+".slf2"));
		
		Downloader downloader = new Downloader( new RepositorySet(remoteRepoUrls, connectionsPerRemote), localRepo, fullyCachedTreeUrns );
		downloader.scanMode = scanMode;
		downloader.reportDownloads = reportDownloads;
		downloader.reportUnrecursableBlobs = reportUnrecursableBlobs;
		downloader.reportDownloadFailures = reportDownloadFailures;
		downloader.reportErrors = reportErrors;
		downloader.reportFailures = reportFailures;
		downloader.setRememberAttempts(rememberAttempts);
		
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
