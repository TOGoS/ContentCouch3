package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import togos.blob.ByteBlob;
import togos.blob.util.BlobUtil;
import togos.ccouch3.BlobReferenceScanner.ScanCallback;
import togos.ccouch3.FlowUploader.Indexer.IndexedObjectSink;
import togos.ccouch3.FlowUploader.StandardTransferTracker.Counter;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.StreamURNifier;
import togos.ccouch3.repo.FileResolver;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.AddableSet;
import togos.ccouch3.util.EmptyAddableSet;
import togos.ccouch3.util.FileUtil;
import togos.ccouch3.util.LogUtil;
import togos.ccouch3.util.RepoURLDefuzzer;
import togos.ccouch3.util.SLFStringSet;

interface FlowUploaderSettings {
	public boolean isDebugging();
	public boolean isFastExitEnabled();
	public Logger getLogger();
}

public class FlowUploader implements FlowUploaderSettings
{
	static class Actions {
		public static final int SKIP_THE_FILE = 2;
		public static final int THROW_AN_EXCEPTION = 1;
	}
	
	static class FileReadError extends IOException {
		private static final long serialVersionUID = 4457136769951017923L;
		public final File file;
		public FileReadError( File file, Exception cause ) {
			super(cause);
			this.file = file;
		}
	}
	
	//// Queue stuff ////
	
	interface Sink<E> {
		public void give( E value ) throws Exception;
	}
	
	static class QueueSink<E> implements Sink<E> {
		protected final BlockingQueue<E> q;
		public QueueSink( BlockingQueue<E> q ) { this.q = q; }
		public void give(E value) throws Exception { q.put(value); }
	}
	
	static abstract class QueueRunner implements Runnable {
		BlockingQueue<Object> inQueue;
		public QueueRunner( BlockingQueue<Object> inQueue ) {
			this.inQueue = inQueue;
		}
		/**
		 * Should return true if it expects to handle more messages.
		 * False indicates that this QueueRunner should clean up and
		 * quit, and handleMessage expects to never be called again.
		 */
		protected abstract boolean handleMessage( Object m ) throws Exception;
		protected abstract void cleanUp() throws Exception;
		public void run() {
			Object m;
			try {
				while( (m = inQueue.take()) != null && handleMessage(m) );
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
			} catch( Exception e ) {
				throw new RuntimeException(e);
			} finally {
				try {
					cleanUp();
				} catch( Exception e ) {
					System.err.print("Error in "+getClass()+" cleanup: ");
					e.printStackTrace(System.err);
				}
			}
		}
	}
	
	interface TransferTracker {
		public static final String TAG_TREENODE = "treenode";
		public static final String TAG_FILE = "file";
		
		// Units = e.g. files, or whatever you want to track.
		public void transferred( long byteCount, int unitCount, String tag );
		public void sendingFile( String filename );
	}
	
	static class StandardTransferTracker implements TransferTracker {
		class Counter {
			long byteCount = 0;
			long unitCount = 0;
		}
		
		protected long totalByteCount = 0, totalUnitCount = 0;
		protected HashMap<String,Counter> counters = new HashMap<String,Counter>();
		protected String currentFilename;
		
		protected Counter getCounter( String name ) {
			Counter c = counters.get(name);
			if( c == null ) counters.put( name, c = new Counter() );
			return c;
		}
		
		@Override
		public synchronized void transferred(long byteCount, int unitCount, String tag) {
			totalByteCount += byteCount;
			totalUnitCount += unitCount;
			Counter c = getCounter(tag);
			c.byteCount += byteCount;
			c.unitCount += unitCount;
		}
		
		public void sendingFile( String filename ) {  currentFilename = filename;  }
		
		public String getCurrentFilename() {  return currentFilename;  }
		public long getTotalByteCount() {  return totalByteCount;  }
		public long getTotalUnitCount() {  return totalUnitCount;  }
	}
	
	//// Message types ////
	
	/**
	 * Information about a commit not including the target URN and date
	 * (which may have yet to be calculated)
	 */
	static class CommitConfig {
		public final String headName;
		public final String description;
		public final String authorName;
		public final String[] tags;
		
		public CommitConfig( String headName, String description, String authorName, String[] tags ) {
			this.headName = headName;
			this.description = description;
			this.authorName = authorName;
			this.tags = tags;
		}
		
		public Commit toCommit( String targetUrn, String[] parentCommitUrns, long timestamp ) {
			return new Commit( targetUrn, parentCommitUrns, tags, authorName, description, timestamp );
		}
	}
	
	static class UploadTask {
		public final String name;
		public final String path;
		/** If non-null, will be used to create commits */
		public final CommitConfig commitConfig;
		
		public UploadTask( String name, String path, CommitConfig commitInfo ) {
			this.name = name;
			this.path = path;
			this.commitConfig = commitInfo;
		}
		
		@Override
		public String toString() { return "UploadTask name="+name+", path="+path; }
	}
	
	/** Marks the end of a stream of messages, usually indicating that the recipient can quit. */
	static class EndMessage {
		public static final EndMessage INSTANCE = new EndMessage();
		private EndMessage() {}
		@Override
		public String toString() { return "EndMessage"; }
	}
	
	static class FullyStoredMarker {
		public final String urn;
		public FullyStoredMarker( String urn ) { this.urn = urn; }
	}
	
	static class LogMessage {
		public final byte[] message;
		public LogMessage( byte[] message ) { this.message = message; }
	}
	
	static class IndexResult {
		public final String inputName;
		public final ByteBlob blob;
		public final String urn;
		public final boolean anyNewData;
		public final boolean fullyStored;
		public IndexResult( String inputName, ByteBlob blob, String urn, boolean anyNewData, boolean fullyStored ) {
			this.inputName = inputName;
			this.blob = blob;
			this.urn = urn;
			this.anyNewData = anyNewData;
			this.fullyStored = fullyStored;
		}
		public FSObjectType getFsObjectType() {
			return FSObjectType.BLOB;
		}
	}
	
	static class FileIndexResult extends IndexResult {
		public final FileInfo fileInfo;
		
		public FileIndexResult( FileInfo fi, boolean anyNewData, boolean fullyStored ) {
			super(fi.getPath(), fi, fi.getUrn(), anyNewData, fullyStored);
			this.fileInfo = fi;
		}
		
		public FSObjectType getFsObjectType() {
			return fileInfo.getFsObjectType();
		}
	}
	
	static class DirectoryContentsIndexResult {
		public final Collection<DirectoryEntry> entries;
		public final boolean fullyStored;
		public DirectoryContentsIndexResult( Collection<DirectoryEntry> entries, boolean fullyStored ) {
			this.entries = entries;
			this.fullyStored = fullyStored;
		}
	}
	
	static class FileStatus {
		public final String path;
		public final String urn;
		public final boolean exists;
		
		public FileStatus( String path, String urn, boolean exists ) {
			this.path = path;
			this.urn = urn;
			this.exists = exists;
		}
		
		@Override
		public String toString() {
			return "FileMissing path="+path+", urn="+urn;
		}
	}
	
	static class PutHead {
		public final String name;
		public final int number;
		public final String headDataUrn;
		
		public PutHead( String name, int minNumber, String headDataUrn ) {
			this.name = name;
			this.number = minNumber;
			this.headDataUrn = headDataUrn;
		}
	}
	
	////
	
	// TODO: split into URN cache and upload cache; they are sometimes needed independently
	
	static class Indexer
	{
		public static class IndexedObjectSink implements Sink<Object> {
			public final Sink<Object> indexResultSink;
			public final AddableSet<String> uploadCache;
			public final String name;
			
			public IndexedObjectSink( AddableSet<String> uc, Sink<Object> sink, String name ) {
				this.uploadCache = uc;
				this.indexResultSink = sink;
				this.name = name;
			}
			
			public boolean contains( String urn ) {
				return uploadCache.contains(urn);
			}
			
			public void give( Object thing ) throws Exception {
				indexResultSink.give(thing);
			}

			public String toString() {
				return "IndexedObjectSink '"+name+"'";
			}
		}
		
		/** Used to find blobs refernced by other blobs. */
		protected final FileResolver localUrnBlobResolver;
		/** Used to find the files mentioned on the command-line. */
		protected final FileResolver localFileResolver;
		protected final BlobReferenceScanMode scanMode;
		protected final DirectorySerializer dirSer;
		protected final StreamURNifier digestor;
		protected final HashCache hashCache;
		protected final int howToHandleFileReadErrors;
		protected final boolean debug;

		public Indexer(
			FileResolver localUrnBlobResolver,
			FileResolver localFileResolver,
			BlobReferenceScanMode scanMode,
			DirectorySerializer dirSer, StreamURNifier digestor,
			HashCache hashCache,
			int howToHandleFileReadErrors, boolean debug
		) {
			this.localUrnBlobResolver = localUrnBlobResolver;
			this.localFileResolver = localFileResolver;
			this.scanMode = scanMode;
			this.dirSer = dirSer;
			this.digestor = digestor;
			this.hashCache = hashCache;
			this.howToHandleFileReadErrors = howToHandleFileReadErrors;
			this.debug = debug;
		}
				
		public DirectoryContentsIndexResult indexDirectoryEntries( File file, Collection<IndexedObjectSink> destinations, Glob ignores) throws Exception {
			assert file.isDirectory();
			
			ArrayList<DirectoryEntry> entries = new ArrayList<DirectoryEntry>();
			
			File[] dirEntries = file.listFiles();
			if( dirEntries == null ) {
				switch( howToHandleFileReadErrors ) {
				case Actions.SKIP_THE_FILE:
					System.err.println("Skipping directory because couldn't list files within (.listFiles() returned null): "+file);
					// Note that 'fully stored' is true, because the entries
					// that we found (an empty set) are all fully stored.
					// 'fullyStored' only refers to the things we actually found.
					return new DirectoryContentsIndexResult(entries, true);
				case Actions.THROW_AN_EXCEPTION:
					throw new Exception("Failed to read files from directory (.listFiles() returned null): "+file);
				default: throw new Exception("Invalid file read error handling option: "+howToHandleFileReadErrors);
				}
			}
			
			boolean allEntriesFullyStored = true;
			for( File c : dirEntries ) {
				if( FileUtil.shouldIgnore(ignores, c) ) continue;
				
				FileIndexResult indexResult;
				try {
					indexResult = index(c, destinations, ignores);
					allEntriesFullyStored &= indexResult.fullyStored;
				} catch( FileReadError e ) {
					switch( howToHandleFileReadErrors ) {
					case Actions.SKIP_THE_FILE:
						System.err.println("Skipping file due to read errors: "+e.file+": "+e.getCause().getMessage());
						continue;
					case Actions.THROW_AN_EXCEPTION: throw e;
					default: throw new Exception("Invalid file read error handling option: "+howToHandleFileReadErrors);
					}
				}
				entries.add( new DirectoryEntry( c.getName(), indexResult.fileInfo ) );
			}
			
			return new DirectoryContentsIndexResult(entries, allEntriesFullyStored);
		}
		
		BlobReferenceScanner brs = null;
		protected BlobReferenceScanner getBlobReferenceScanner() {
			if( brs == null ) {
				brs = new BlobReferenceScanner(scanMode);
				brs.reportErrors = true; // Yeah always, we don't have a -quiet option
				brs.reportUnrecursableBlobs = debug;
			}
			return brs;
		}
		
		/**
		 * Returns true if all references found could be resolved
		 * to blobs and passed on.
		 */
		public boolean scanBlobReferences( final BlobInfo blob, final Collection<IndexedObjectSink> destinations ) {
			switch( scanMode ) {
			case NEVER:
				return true;
			case SCAN_TEXT_FOR_URNS:
			case SCAN_TEXT_FOR_RDF_OBJECT_URNS:
				try {
					return getBlobReferenceScanner().scanTextForUrns(blob.getUrn(), blob.openInputStream(), new ScanCallback() {
						@Override public boolean handle(String urn) {
							if( debug ) System.err.println("Found "+urn+" referenced by "+blob.getUrn());
							return indexBlobByUrn(urn, destinations);
						}
					}, false);
				} catch( IOException e ) {
					System.err.println("Failed to recurse into "+blob.getUrn()+": "+e.getMessage());
					return false;
				}
			default:
				System.err.println(scanMode+" scan mode not supported!");
				return false;
			}
		}
		
		protected Collection<IndexedObjectSink> getNeedySinks(String urn, Collection<IndexedObjectSink> destinations) throws Exception {
			Collection<IndexedObjectSink> needy = Collections.emptyList();
			
			for( IndexedObjectSink d : destinations ) {
				if( !d.contains(urn) ) {
					if( needy.size() == 0 ) needy = new ArrayList<IndexedObjectSink>();
					needy.add(d);
				}
			}
			
			return needy;
		}
		
		protected byte[] serializeDirectory( Collection<DirectoryEntry> entries ) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				dirSer.serialize(entries, baos);
				baos.close();
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
			return baos.toByteArray();
		}
		
		// TODO: Probably want to refactor
		// so that index(blob file) calls index(URN),
		// not the other way around.
		//
		// Right now, URNs must be backed by files to be uploaded, and
		// once the file is found, the original URN gets ignored and
		// the URN has to be re-calculated!
		
		protected IndexResult index( final String name, final ByteBlob blob, Collection<IndexedObjectSink> destinations ) throws Exception {
			InputStream is = blob.openInputStream();
			final String urn;
			try {
				urn = digestor.digest(is);
			} finally {
				is.close();
			}
			
			BlobInfo bi = new BlobInfo() {
				public String getUrn() {
					return urn;
				}
				@Override public long getSize() {
					return blob.getSize();
				}
				@Override public InputStream openInputStream() throws IOException {
					return blob.openInputStream();
				}
				@Override public ByteBlob slice(long offset, long length) {
					return blob.slice(offset, length);
				}
				@Override public void writeTo(OutputStream os) throws IOException {
					blob.writeTo(os);
				}
			};
			
			// Copy-pasta from index( File, ... )
			
			destinations = getNeedySinks(urn, destinations);
			if( destinations.size() == 0 ) {
				if( debug ) System.err.println("Indexer: "+urn+" already uploaded"); 
				return new IndexResult( name, bi, urn, false, true );
			}
			
			if( debug ) System.err.println("Indexer: Some destinations don't yet have "+urn);
			
			for( IndexedObjectSink d : destinations ) d.give(bi);
			
			boolean fullyUploaded = scanBlobReferences( bi, destinations );
			
			if( fullyUploaded ) {
				if( debug ) System.err.println("Indexer: Marking "+urn+" as fully uploaded");
				for( IndexedObjectSink d : destinations ) {
					d.give( new FullyStoredMarker(urn) );
				}
			}
			
			return new IndexResult(name, bi, urn, true, fullyUploaded);
		}
		
		protected FileIndexResult index( File file, Collection<IndexedObjectSink> destinations, final Glob ignores ) throws Exception {
			if( debug ) System.err.println("Indexer: "+file+"...");
			String cachedUrn = hashCache.getFileUrn( file );
			if( debug ) {
				if( cachedUrn == null ) {
					System.err.println("Indexer: Hash cache has no entry for "+file);
				} else {
					System.err.println("Indexer: Hash cache says "+file+" = "+cachedUrn);
				}
			}
			if( cachedUrn != null && (destinations = getNeedySinks(cachedUrn, destinations)).size() == 0 ) {
				if( debug ) System.err.println("Indexer: "+cachedUrn+" already uploaded everywhere!");
				// Then we don't need to recurse into subdirectories!
				return new FileIndexResult(
					new FileInfo(
						file.getCanonicalPath(),
						cachedUrn,
						file.isDirectory() ? FSObjectType.DIRECTORY : FSObjectType.BLOB,
						file.length(),
						file.lastModified()
					), false, true
				);
			}
			
			final boolean fullyUploaded;
			FileInfo fi;
			if( file.isFile() ) {
				String fileUrn;
				if( cachedUrn != null ) {
					fileUrn = cachedUrn;
				} else {
					FileInputStream fis = null;
					try {
						fis = new FileInputStream( file );
						fileUrn = digestor.digest(fis);
						hashCache.cacheFileUrn( file, fileUrn );
					} catch( IOException e ) {
						throw new FileReadError( file, e );
					} finally {
						if( fis != null ) fis.close();
					}
				}
				
				if( debug ) System.err.println("Indexer: "+file+" = "+fileUrn);
				
				fi = new FileInfo(
					file.getCanonicalPath(),
					fileUrn,
					FSObjectType.BLOB,
					file.length(),
					file.lastModified()
				);
				
				destinations = getNeedySinks(fi.getUrn(), destinations);
				if( destinations.size() == 0 ) {
					if( debug ) System.err.println("Indexer: "+file+": already uploaded: "+fileUrn); 
					return new FileIndexResult( fi, false, true );
				}
				
				if( debug ) System.err.println("Indexer: Some destinations don't yet have "+fi.getUrn());
				
				for( IndexedObjectSink d : destinations ) d.give(fi);
				
				fullyUploaded = scanBlobReferences( fi, destinations );
				
				if( fullyUploaded ) {
					for( IndexedObjectSink d : destinations ) {
						if( debug ) System.err.println("Indexer: Marking file "+fileUrn+" fully uploaded");
						d.give( new FullyStoredMarker(fileUrn) );
					}
				}
			} else if( file.isDirectory() ) {
				File ignoreFile = new File(file, ".ccouchignore");
				Glob myIgnores = ignoreFile.exists() ? Glob.load(ignoreFile, ignores) : ignores;
				
				DirectoryContentsIndexResult contentsIndexResult = indexDirectoryEntries( file, destinations, myIgnores );
				fullyUploaded = contentsIndexResult.fullyStored;
				
				byte[] serialized = serializeDirectory(contentsIndexResult.entries);
				
				String rdfBlobUrn = digestor.digest(new ByteArrayInputStream(serialized));
				String treeUrn = "x-rdf-subject:" + rdfBlobUrn;
				
				fi = new FileInfo(
					file.getCanonicalPath(),
					treeUrn,
					FSObjectType.DIRECTORY,
					file.length(),
					file.lastModified()
				);
				
				destinations = getNeedySinks(treeUrn, destinations);
				
				if( destinations.size() == 0 ) {
					if( debug ) System.err.println("Indexer: "+file+": already uploaded: "+treeUrn); 
					return new FileIndexResult( fi, false, true );
				}
				
				SmallBlobInfo blobInfo = new SmallBlobInfo( rdfBlobUrn, serialized );
				
				for( IndexedObjectSink d : destinations ) {
					if( !d.contains(fi.getUrn()) ) {
						d.give( blobInfo );
						if( fullyUploaded ) {
							if( debug ) System.err.println("Indexer: Sending FullyStoredMarker("+treeUrn+") to "+d);
							d.give( new FullyStoredMarker(treeUrn) );
						}
					}
				}
			} else if( !file.exists() ) {
				throw new RuntimeException(file+" does not exist");
			} else {
				throw new RuntimeException("Don't know how to index "+file);
			}
			if( debug ) System.err.println("Indexer: "+file+": done: "+fi.getUrn());
			return new FileIndexResult( fi, true, fullyUploaded );
		}
		
		// This is to prevent tight loops where the same blob is
		// processed again and again and again due to being referenced
		// a lot.
		// 
		// The 'fully uploaded' markers might not come back for a while,
		// so we can't rely on them to catch cases where e.g.
		// one blob references one other blob 1000 times.
		
		protected HashMap<String,Boolean> urnsAlreadyIndexed = new HashMap<String,Boolean>();
		
		protected void indexedBlobByUrn( String urn, boolean result ) {
			if( urnsAlreadyIndexed.size() >= 1024 ) {
				// Chop it down a bit so we don't consume all the memory.
				boolean rem = true;
				for( Iterator<Map.Entry<String,Boolean>> i = urnsAlreadyIndexed.entrySet().iterator(); i.hasNext(); ) {
					i.next();
					if(rem) i.remove();
					rem ^= true;
				}
				if(debug) System.err.println(
					"Chopped already-indexed URN set down to "+
					urnsAlreadyIndexed.size()+" entries.");
			}
			urnsAlreadyIndexed.put(urn, Boolean.valueOf(result));
		}
		
		/**
		 * Used by callers who already know the URN of the thing,
		 * and that it's a blob.
		 * 
		 * Processes the blob and returns true if all referenced
		 * (based on scanMode) blobs were successfully scanned
		 * or already fully stored everywhere.
		 */
		protected boolean indexBlobByUrn( String urn, Collection<IndexedObjectSink> destinations ) {
			Boolean b = urnsAlreadyIndexed.get(urn);
			if( b != null ) return b.booleanValue();
			
			// Note: There's no reason this needs to be a file.
			// Could go all indexBlob(), here.
			File f;
			try {
				f = localUrnBlobResolver.getFile(urn);
			} catch( IOException e ) {
				System.err.println(e.getMessage());
				indexedBlobByUrn(urn, false);
				return false;
			}
			try {
				boolean success = index(f, destinations, FileUtil.DEFAULT_IGNORES).fullyStored;
				indexedBlobByUrn(urn, success);
				return success;
			} catch( Exception e ) {
				System.err.println("Error while processing "+f+": "+e.getMessage());
				return false;
			}
		}
		
		// TODO: Should probably just make a ThingResolver class to do this.
		protected Object get( String name ) throws IOException {
			try {
				if( localFileResolver instanceof BlobResolver ) {
					ByteBlob b = ((BlobResolver)localFileResolver).getBlob(name);
					if( b != null ) return b;
				}
			} catch( FileNotFoundException e ) {
			}
			try {
				File f = localFileResolver.getFile(name);
				if( f != null ) return f;
			} catch( FileNotFoundException e ) {
			}
			throw new FileNotFoundException(name);
		}
		
		protected IndexResult index( String path, Collection<IndexedObjectSink> destinations ) throws Exception {
			Object thing = get(path);
			if( thing instanceof File ) {
				return index( (File)thing, destinations, FileUtil.DEFAULT_IGNORES);
			} else if( thing instanceof ByteBlob ) {
				return index( path, (ByteBlob)thing, destinations );
			} else {
				throw new FileNotFoundException("Resolving '"+path+"' gave me a "+thing.getClass()+", which I don't know what to do with");
			}
		}
	}
	
	static class FlowUploaderConfig {
		public Collection<UploadTask> tasks = new ArrayList<UploadTask>();
		public int howToHandleFileReadErrors = Actions.THROW_AN_EXCEPTION;
		public boolean debug = false;
		public boolean fastExitEnabled = false;
		public boolean showTransferSummary = false;
		public boolean showProgress = false;
		public boolean reportPathUrnMapping = false;
		public boolean reportUrn = false;
		public boolean includeFileMtimes = true;
		public StreamURNifier digestor = BitprintDigest.STREAM_URNIFIER;
		public File primaryRepoDir;
		public File cacheDir;
		public File dataDir;
		public File headDir;
		public String storeSector = "user";
		public Collection<UploadClientSpec> uploadClientSpecs = new ArrayList<UploadClientSpec>();
		public BlobReferenceScanMode scanMode = BlobReferenceScanMode.NEVER;
		
		public static FlowUploaderConfig from(CCouchContext ctx) {
			FlowUploaderConfig config = new FlowUploaderConfig();
			config.primaryRepoDir = ctx.getPrimaryRepoDir(null);
			return config;
		}
	}
	
	//// Put it all together ////
	
	protected final Collection<UploadTask> tasks;
	protected final int howToHandleFileReadErrors;
	protected final boolean debug;
	protected final boolean fastExitEnabled;
	protected final boolean showTransferSummary;
	protected final boolean showProgress;
	protected final boolean reportPathUrnMapping;
	protected final boolean reportUrn;
	protected final StreamURNifier digestor;
	protected final DirectorySerializer dirSer;
	protected final File primaryRepoDir;
	protected final File cacheDir;
	protected final BlobReferenceScanMode scanMode;
	
	// Used when creating commits:
	protected final File dataDir;
	protected final File headDir;
	protected final String storeSector;
	
	// Used when uploading:
	protected final UploadClientSpec[] uploadClientSpecs;
	
	public FlowUploader( FlowUploaderConfig config ) {
		this.tasks = config.tasks;
		this.howToHandleFileReadErrors = config.howToHandleFileReadErrors;
		this.debug = config.debug;
		this.fastExitEnabled = config.fastExitEnabled;
		this.showTransferSummary = config.showTransferSummary;
		this.showProgress = config.showProgress;
		this.reportPathUrnMapping = config.reportPathUrnMapping;
		this.reportUrn = config.reportUrn;
		this.digestor = config.digestor;
		this.dirSer = new NewStyleRDFDirectorySerializer(config.includeFileMtimes);
		this.primaryRepoDir = config.primaryRepoDir;
		this.cacheDir = config.cacheDir;
		this.dataDir = config.dataDir;
		this.headDir = config.headDir;
		this.storeSector = config.storeSector;
		this.uploadClientSpecs = config.uploadClientSpecs.toArray(new UploadClientSpec[config.uploadClientSpecs.size()]);
		this.scanMode = config.scanMode;
	}
	
	public boolean isDebugging() {
		return debug;
	}
	public boolean isFastExitEnabled() {
		return fastExitEnabled;
	}
	protected Logger logger;
	public Logger getLogger() {
		if( logger == null ) {
			logger = new NormalLogger(new File(primaryRepoDir, "log"));
		}
		return logger;
	}
	
	protected HashCache hashCache;
	protected synchronized HashCache getHashCache() {
		if( cacheDir == null ) {
			return new HashCache() {
				@Override public String getFileUrn(File f) throws Exception { return null; }
				@Override public void cacheFileUrn(File f, String urn) throws Exception {}
			};
		}
		if( hashCache == null ) {
			hashCache = new SLFHashCache(cacheDir);
		}
		return hashCache;
	}
	
	protected Map<String,AddableSet<String>> uploadCaches = new HashMap<String,AddableSet<String>>();
	protected synchronized AddableSet<String> getUploadCache( String serverName, BlobReferenceScanMode scanMode ) {
		if( cacheDir == null ) return EmptyAddableSet.getInstance();
		AddableSet<String> uc = uploadCaches.get(serverName);
		if( uc == null ) {
			uploadCaches.put( serverName, uc = new SLFStringSet(new File(cacheDir,
				"uploaded-to-"+serverName+(scanMode == BlobReferenceScanMode.NEVER ? "" : "-rs-"+scanMode.cacheDbName.toLowerCase())+".slf2")) );
			// 'rs' = 'recursively scanned'
		}
		return uc;
	}
	
	protected Repository localRepository;
	protected synchronized Repository getLocalRepository() {
		if( localRepository == null ) {
			if( dataDir == null ) throw new RuntimeException("Can't instantiate local repository; dataDir is null");
			if( storeSector == null ) throw new RuntimeException("Can't instantiate local repository; storeSector is null");
			localRepository = new SHA1FileRepository(dataDir, storeSector);
		}
		return localRepository;
	}
	
	protected FileResolver localFileResolver;
	protected synchronized FileResolver getLocalFileResolver() {
		if( localFileResolver == null ) {
			final Repository localRepo = getLocalRepository();
			// Ack, the hack!:
			// TODO: Refactor to be less dumb,
			// allow secondary (read-only)t repositories
			localFileResolver = CCouch3Command.getCommandLineFileResolver(
				new Repository[]{ localRepo },
				new File[] { headDir.getParentFile() }
			);
		}
		return localFileResolver;
	}
	
	protected CommitManager cman;
	protected synchronized CommitManager getCommitManager() {
		if( cman == null ) {
			cman = new CommitManager( getLocalRepository(), digestor );
		}
		return cman;
	}
	
	protected HeadManager hman;
	protected synchronized HeadManager getHeadManager() {
		if( hman == null ) {
			if( headDir == null ) throw new RuntimeException("Can't instantiate head manager; headDir is null");
			hman = new HeadManager(headDir);
		}
		return hman;
	}
	
	protected void report( IndexResult indexResult ) {
		if( reportUrn ) {
			System.out.println(indexResult.urn);
		}
		if( reportPathUrnMapping ) {
			System.out.println(indexResult.inputName+"\t"+indexResult.urn);
		}
	}
	
	public void runIdentify() throws Exception {
		final Indexer indexer = new Indexer( getLocalRepository(), getLocalFileResolver(), BlobReferenceScanMode.NEVER, dirSer, digestor, getHashCache(), howToHandleFileReadErrors, debug );
		for( UploadTask ut : tasks ) {
			IndexResult indexResult = indexer.index(ut.path, Collections.<IndexedObjectSink>emptyList());
			report( indexResult );
		}
	}
	
	protected static void pipe( InputStream is, OutputStream os ) throws IOException {
		byte[] buffer = new byte[65536];
		for( int z = is.read(buffer); z>0; z = is.read(buffer) ) {
			os.write(buffer, 0, z);
		}
	}
	
	static class Piper extends Thread {
		protected final InputStream is;
		protected final OutputStream os;
		
		public Piper( InputStream is, OutputStream os ) {
			this.is = is;
			this.os = os;
		}
		
		@Override
		public void interrupt() {
			super.interrupt();
			try {
				is.close();
			} catch( IOException e ) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			try {
				pipe(is,os);
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	boolean anythingSent = false;
	
	/**
	 * This method creates, starts, and waits for a bunch of threads that comminicate via
	 * pipes and queues.  The code does not make this super obvious, but the flow
	 * is fairly simple:
	 * 
	 * indexer -> head requestor -> head response handler -> uploader -> upload response handler
	 *                                \-> head error piper                 \-> upload error piper
	 *                                
	 * if indexer finishes without sending anything on to the next process, the other threads
	 * can be aborted.
	 */
	public int runUpload() {
		final StandardTransferTracker tt = new StandardTransferTracker();
		final UploadClient[] uploadClients = new UploadClient[uploadClientSpecs.length];
		final ArrayList<IndexedObjectSink> indexedObjectSinks = new ArrayList<IndexedObjectSink>(uploadClientSpecs.length);
		for( int i=0; i<uploadClientSpecs.length; ++i ) {
			String serverName = uploadClientSpecs[i].getServerName();
			final AddableSet<String> uc = getUploadCache(serverName, scanMode);
			uploadClients[i] = uploadClientSpecs[i].createClient( uc, tt, this );
			indexedObjectSinks.add(new IndexedObjectSink( uc, uploadClients[i], serverName ));
			if( debug ) {
				System.err.println("For remote server '"+serverName+"'...");
				System.err.println("  Using "+uc+" to remember fully-uploaded blobs");
				System.err.println("  Created "+uploadClients[i].toString());
			}
		}
		
		final LinkedBlockingQueue<Object> uploadTaskQueue = new LinkedBlockingQueue<Object>(tasks);
		uploadTaskQueue.add( EndMessage.INSTANCE );

		final Indexer indexer = new Indexer( getLocalRepository(), getLocalFileResolver(), scanMode, dirSer, digestor, getHashCache(), howToHandleFileReadErrors, debug );
		
		class IndexRunner extends QueueRunner {
			private boolean success = true;
			private boolean completed = false;
			
			public IndexRunner( LinkedBlockingQueue<Object> q ) {
				super(q);
			}
			
			public boolean handleMessage( Object m ) throws Exception {
				if( m instanceof UploadTask ) {
					UploadTask ut = (UploadTask)m;
					IndexResult indexResult;
					try {
						indexResult = indexer.index(ut.path, indexedObjectSinks);
					} catch( FileNotFoundException e ) {
						System.err.println("Error: "+e.getMessage()+"; noting this failure, I shall carry on.");
						success = false;
						return true;
					}
					synchronized(this) {
						success &= indexResult.fullyStored;
					}
					long timestamp = System.currentTimeMillis();
					report( indexResult );
					
					if( indexResult.anyNewData ) {
						// If any new data was uploaded, send the name -> URN mapping to the server
						// to be logged.  We want to NOT do this if we are only indexing and not
						// sending!  In this case anyNewData will also be false.
						String message = LogUtil.formatStorageLogEntry(new Date(), indexResult.getFsObjectType(), ut.name, indexResult.urn);
						LogMessage lm = new LogMessage(BlobUtil.bytes(message));
						for( IndexedObjectSink d : indexedObjectSinks ) d.give( lm );
					}
					
					if( ut.commitConfig != null ) {
						CommitManager.CommitSaveResult csr = getCommitManager().saveCommit(
							new File(ut.path), indexResult.urn, timestamp, ut.commitConfig );
						
						SmallBlobInfo commitBlobInfo = new SmallBlobInfo( csr.latestCommitDataUrn, csr.latestCommitData );
						for( IndexedObjectSink d : indexedObjectSinks ) {
							// Log the commit and send the data to any server that doesn't have it
							if( !d.contains(csr.latestCommitDataUrn) ) {
								String message =
										"[" + new Date(System.currentTimeMillis()).toString() + "] Uploaded\n" +
										"Commit '" + ut.name + "' = " + csr.latestCommitUrn;
									LogMessage lm = new LogMessage(BlobUtil.bytes(message));
									d.give( lm );
								d.give( commitBlobInfo );
								// 2021-11-07: this seems wrong, so commenting-out:
								//d.give( new FullyStoredMarker(csr.latestCommitDataUrn) );
							}
							
						}
						
						if( ut.commitConfig.headName != null ) {
							int headNum = getHeadManager().addHead(ut.commitConfig.headName, 0, csr.latestCommitData);
							PutHead ph = new PutHead( ut.commitConfig.headName, headNum, csr.latestCommitDataUrn );
							String headNameUrn = "ccouch-head:"+ut.commitConfig.headName+"/"+headNum;
							for( IndexedObjectSink d : indexedObjectSinks ) {
								// Send the head to any server that doesn't have it
								if( !d.contains(headNameUrn) ) {
									d.give( ph );
									// 2021-11-07: this seems wrong, so commenting-out:
									//d.give( new FullyStoredMarker(headNameUrn) );
								}
							}
						}
					}
					
					return true;
				} else if( m instanceof EndMessage ) {
					synchronized(this) { completed = true; }
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
			
			@Override
			protected void cleanUp() throws Exception {
				for( IndexedObjectSink d : indexedObjectSinks ) {
					if( debug ) System.err.println("Indexer: Sending EndMessage to IndexedObjectSink.");
					d.give( EndMessage.INSTANCE );
				}
			}
			
			public synchronized boolean completedSuccessfully() {
				return completed && success;
			}
		}
		final IndexRunner indexRunner = new IndexRunner(uploadTaskQueue);
		
		final Thread indexThread = new Thread( indexRunner, "Indexer" );
		final Thread progressThread = new Thread() {
			public void run() {
				try {
					while( !Thread.interrupted() ) {
						String fn = tt.getCurrentFilename();
						if( fn == null ) fn = "";
						if( fn.length() > 53 ) fn = "..."+fn.substring( fn.length()-50 );
						System.err.print( String.format("% 10d / % 7d ; %50s\r", tt.getTotalByteCount(), tt.getTotalUnitCount(), fn) );
						System.err.flush();
						Thread.sleep(1000);
					}
				} catch( InterruptedException e ) {}
			}
		};
		
		for( UploadClient uc : uploadClients ) uc.start();
		// UploadClients must be initialized (uc.start() must take care of this before it returns)
		// before anything can be written to them, which is why I start them before starting
		// the index thread!
		indexThread.start();
		if( showProgress ) progressThread.start();
		
		boolean completeSuccess = true;
		try {
			if( debug ) System.err.println("Waiting for upload clients to finish...");
			for( UploadClient uc : uploadClients ) uc.join();
			if( debug ) System.err.println("Upload clients all completed.");
			
			if( debug ) System.err.println("Waiting for index thread to finish...");
			indexThread.join(1000);
			if( indexThread.isAlive() ) {
				if( debug ) System.err.println("Killing index thread.");
				indexThread.interrupt();
			}
			indexThread.join();
			if( debug ) System.err.println("Index thread completed.");
			
			progressThread.interrupt();
			
			if( !indexRunner.completedSuccessfully() ) {
				System.err.println("Not everything indexed could be queued for upload.");
				completeSuccess = false;
			}
			
			for( UploadClient uc : uploadClients ) {
				if( !uc.completedSuccessfully() ) {
					completeSuccess = false;
				}
				if( uc instanceof CommandUploadClient ) {
					// Get more specific
					CommandUploadClient cuc = (CommandUploadClient)uc; 
					if( cuc.anythingSent ) {
						if( cuc.headProcExitCode != 0 ) {
							System.err.println("Error: "+uc.getServerName()+" head process exited with code "+cuc.headProcExitCode);
							completeSuccess = false;
						}
						if( cuc.uploadProcExitCode != 0 ) {
							System.err.println("Error: "+uc.getServerName()+" upload process exited with code "+cuc.uploadProcExitCode);
							completeSuccess = false;
						}
					}
				}
			}
		} catch( InterruptedException e ) {
			System.err.println("Interrupted!");
			completeSuccess = false;
			indexThread.interrupt();
			for( UploadClient uc : uploadClients ) uc.halt();
			Thread.currentThread().interrupt();
			progressThread.interrupt();
		}
		
		if( showTransferSummary ) {
			if( tt.counters.isEmpty() ) {
				System.err.println( "No transfers!" );
			}
			for( Map.Entry<String,Counter> thing : tt.counters.entrySet() ) {
				System.err.println( "Transferred "+thing.getValue().byteCount+" bytes in "+thing.getValue().unitCount+" "+thing.getKey()+"s");
			}
		}
		
		if( debug || !completeSuccess ) {
			System.err.println(completeSuccess ? "All uploads successful!" : "Error: Some uploads did not complete successfully.");
		}
		
		return completeSuccess ? 0 : 1;
	}
	
	static String stripTrailingSlash( String path ) {
		return ( path.length() > 0 && path.charAt(path.length()-1) == '/' ) ?
			path.substring(0, path.length()-1) : path;
	}
	
	/**
	 * Returns an appropriate directory for storing cache files
	 * within the repository at the given path.
	 */
	static File repoCacheDir( File repoDir ) {
		return new File( repoDir, "cache" + File.separator + "flow-uploader");
	}
	
	/**
	 * Returns an appropriate directory for storing data files
	 * (this directory will contain 'sector' directories)
	 * within the repository at the given path.
	 */
	static File repoDataDir( File repoDir ) {
		return new File(repoDir, "data");
	}
	
	/**
	 * Returns an appropriate directory for storing head files
	 * within the repository at the given path.
	 */
	static File repoHeadDir( File repoDir ) {
		return new File(repoDir, "heads");
	}
	
	static class FlowUploaderCommand {
		enum Mode {
			RUN, ERROR, HELP
		};
		
		public static FlowUploaderCommand error( String errorMessage ) {
			return new FlowUploaderCommand( Mode.ERROR, errorMessage, null );
		}
		
		public static FlowUploaderCommand normal( FlowUploaderConfig config ) {
			return new FlowUploaderCommand( Mode.RUN, null, config );
		}

		public static FlowUploaderCommand help() {
			return new FlowUploaderCommand( Mode.HELP, null, null );
		}
		
		public final Mode mode;
		public final String errorMessage;
		public final FlowUploaderConfig config;
		
		public FlowUploaderCommand( Mode mode, String errorMessage, FlowUploaderConfig config ) {
			this.mode = mode;
			this.errorMessage = errorMessage;
			this.config = config;
		}
	}
	
	interface UploadClientSpec
	{
		public String getServerName();
		
		/**
		 * 
		 * @param uc
		 * @param tt
		 * @param fus
		 * @return
		 */
		public UploadClient createClient( AddableSet<String> uc, TransferTracker tt, FlowUploaderSettings fus );
	}
	
	static class CommandUploadClientSpec implements UploadClientSpec {
		public final String name;
		public final String[] command;
		
		public CommandUploadClientSpec( String name, String[] command ) {
			this.name = name;
			this.command = command;
		}
		
		public String getServerName() { return name; }
		
		public UploadClient createClient( AddableSet<String> uc, TransferTracker tt, FlowUploaderSettings fus ) {
			CommandUploadClient cuc = new CommandUploadClient( name, command, uc, tt );
			cuc.debug = fus.isDebugging();
			cuc.dieWhenNothingToSend = fus.isFastExitEnabled();
			return cuc;
		}
	}
	
	static class HTTPUploadClientSpec implements UploadClientSpec {
		public final String name;
		public final String url;
		public int maxUploadAttempts;
		
		public HTTPUploadClientSpec( String name, String url ) {
			this.name = name;
			this.url = url;
		}
		
		public String getServerName() { return name; }
		
		public UploadClient createClient( AddableSet<String> uc, TransferTracker tt, FlowUploaderSettings fus ) {
			HTTPUploadClient huc = new HTTPUploadClient( name, url, uc, tt, fus.getLogger() );
			huc.debug = fus.isDebugging();
			huc.maxUploadAttempts = maxUploadAttempts;
			//huc.dieWhenNothingToSend = fus.isFastExitEnabled();
			return huc;
		}
	}
	
	static class NoEndSemicolon extends Exception {
		private static final long serialVersionUID = 1L;
		
		public final String[] command;
		public NoEndSemicolon( String[] command ) {
			this.command = command;
		}
	}
	
	static String[] readSubCommandArguments( Iterator<String> args ) throws NoEndSemicolon {
		String a;
		List<String> sc = new ArrayList<String>();
		for( a = args.hasNext() ? args.next() : null; a != null && !";".equals(a); a = args.hasNext() ? args.next() : null ) {
			if( a.equals(";;") ) a = ";";
			sc.add( a );
		}
		if( a == null ) throw new NoEndSemicolon(sc.toArray(new String[sc.size()]));
		return sc.toArray(new String[sc.size()]);
	}
	
	static FlowUploaderCommand fromArgs( CCouchContext ctx, Iterator<String> args, boolean requireServer, boolean alwaysShowUrns ) throws Exception {
		ctx.fix();
		FlowUploaderConfig config = FlowUploaderConfig.from(ctx);
		
		boolean verbose = false;
		boolean cacheEnabled = true;
		File cacheDir = null;
		File dataDir = null;
		File headDir = null;
		String serverName = null;
		String[] serverCommand = null;
		
		String repoName = null;
		
		// Optional commit info
		String commitName = null;
		String commitAuthor = null;
		String commitMessage = null;
		
		for( ; args.hasNext(); ) {
			String a = args.next();
			BlobReferenceScanMode parsedScanMode;
			
			// Verbosity options
			if( "-v".equals(a) ) {
				verbose = true;
				config.showProgress = true;
			} else if( "-show-progress".equals(a) ) {
				config.showProgress = true;
			} else if( "-debug".equals(a) ) {
				config.debug = true;
			} else if( "-enable-fast-exit".equals(a) ) {
				config.fastExitEnabled = true;
			
			// Commit options
			} else if( "-m".equals(a) ) {
				commitMessage = args.next();
			} else if( "-a".equals(a) ) {
				commitAuthor = args.next();
			} else if( "-n".equals(a) ) {
				commitName = args.next();
			
			} else if( "-no-cache".equals(a) ) {
				cacheEnabled = false;
			
			// Local repository options
			} else if(
				"-local-repo".equals(a) || a.startsWith("-local-repo:") ||
				"-repo".equals(a) || a.startsWith("-repo:") // For backwards compatibility!
			) {
				if( a.contains(":") ) repoName = a.substring(a.indexOf(":")+1);
				
				config.primaryRepoDir = new File(args.next());
				cacheDir = repoCacheDir( config.primaryRepoDir );
				dataDir  = repoDataDir( config.primaryRepoDir );
				headDir  = repoHeadDir( config.primaryRepoDir );
			} else if( "-cache-dir".equals(a) ) {
				cacheDir = new File(args.next());
			} else if( "-data-dir".equals(a) ) {
				dataDir = new File(args.next());
			} else if( "-sector".equals(a) ) {
				config.storeSector = args.next();
			} else if( "-server-name".equals(a) ) {
				serverName = args.next();
			} else if( a.startsWith("-http-server:") || a.startsWith("-flaky-http-server:") ) {
				final String sn = a.substring(a.indexOf(":")+1);
				HTTPUploadClientSpec hucs = new HTTPUploadClientSpec(sn, RepoURLDefuzzer.defuzzRemoteRepoPrefix(args.next()));
				if( a.startsWith("-flaky") ) {
					hucs.maxUploadAttempts = 4;
				}
				config.uploadClientSpecs.add( hucs );
			} else if( a.startsWith("-command-server:") ) {
				final String sn = a.substring(16);
				try {
					config.uploadClientSpecs.add( new CommandUploadClientSpec(sn, readSubCommandArguments(args)) );
				} catch( NoEndSemicolon e ) {
					String summary = "";
					for( String arg : e.command ) {
						if( summary.length() > 0 ) summary += " ";
						if( arg.contains(" ") ) arg = '"'+arg+'"'; 
						summary += arg;
					}
					if( summary.length() > 40 ) {
						summary = summary.substring(0, 37)+"...";
					}
					return FlowUploaderCommand.error("No ending ';' found after '-server-command': "+summary);
				}
			} else if( "-server-command".equals(a) ) {
				serverCommand = readSubCommandArguments(args);

			// Options about what to upload
			} else if( (parsedScanMode = BlobReferenceScanMode.parseRecurseArg(a)) != null ) {
				config.scanMode = parsedScanMode;
			} else if( "-omit-file-mtimes".equals(a) ) {
				config.includeFileMtimes = false;
			} else if( "-skip-files-with-read-errors".equals(a) ) {
				config.howToHandleFileReadErrors = Actions.SKIP_THE_FILE;
			} else if( !a.startsWith("-") || "-".equals(a) ) {
				CommitConfig commitConfig;
				if( commitAuthor == null && commitMessage == null && commitName == null ) {
					commitConfig = null;
				} else {
					if( commitName != null && repoName == null ) {
						return FlowUploaderCommand.error("Must specify a local repo name (with -local-repo:<repo-name> <repo-path>) before -n <head-name> or use -full-head-name <full-head-name>");
					}
					
					commitConfig = new CommitConfig( repoName + "/" + commitName, commitMessage, commitAuthor, new String[0] );
					commitMessage = null;
					commitName = null;
				}
				
				config.tasks.add( new UploadTask(a, a, commitConfig) );
				
			// Help
			} else if( CCouch3Command.isHelpArgument(a) ) {
				return FlowUploaderCommand.help();
			} else {
				return FlowUploaderCommand.error("Unrecognised argument: "+a);
			}
		}
		
		if( !cacheEnabled ) {
			config.cacheDir = null;
		} else if( cacheDir == null ) {
			config.cacheDir = repoCacheDir(config.primaryRepoDir);
		} else {
			config.cacheDir = cacheDir;
		}
		
		// TODO: Allow multiple data directories for reading
		
		if( dataDir == null ) {
			config.dataDir = repoDataDir(config.primaryRepoDir);
		} else {
			config.dataDir = dataDir;
		}
		
		if( headDir == null ) {
			config.headDir = repoHeadDir(config.primaryRepoDir);
		} else {
			config.headDir = headDir;
		}
		
		// Set up upload clients //
		
		if( (serverName == null) != (serverCommand == null) ) {
			return FlowUploaderCommand.error("-server-name and -server-command must both be specified if either are.");
		}
		if( serverName != null && serverCommand != null ) {
			config.uploadClientSpecs.add( new CommandUploadClientSpec(serverName, serverCommand) );
		}
		if( requireServer ) {
			if( config.uploadClientSpecs.size() == 0 ) {
				return FlowUploaderCommand.error("No servers specified.");
			}
		}
		
		boolean showUrns = verbose || alwaysShowUrns;
		
		config.showTransferSummary = verbose;
		config.reportPathUrnMapping = showUrns && config.tasks.size() != 1;
		config.reportUrn = showUrns && config.tasks.size() == 1;
		
		return FlowUploaderCommand.normal( config );
	}
	
	static final String UPLOAD_USAGE =
		"Usage: ccouch3 upload <options> <file/dir> ...\n" +
		"\n" +
		"Upload files and directories to a repository by piping cmd-server commands\n" +
		"to another program (probably 'ssh somewhere \"ccouch3 cmd-server\"').\n" +
		"\n" +
		"Options:\n" +
		"  -n <name>      ; Give a name for the next commit\n" +
		"  -a <author>    ; Give an author name for the next commit\n" +
		"  -m <message>   ; Give a description for the next commit\n" +
		"  -recurse[:<mode>] ; Recursively scan blobs for URN references and upload\n" +
		"                 ; their targets, too (this is a bit of a misnomer, since\n" +
		"                 ; directories are always recursively uploaded)\n" +
		"  -recurse:?     ; Dump recursion mode options.\n" +
		"  -local-repo <path> ; Path to local ccouch repository to store cache in.\n" +
		"  -local-repo:<name> <path> ; Path to a named local repository; this is needed\n" +
		"                 ; when creating a named commit with '-n'\n" +
		"  -no-cache      ; Do not cache file hashes or upload records.\n" +
		"  -omit-file-mtimes ; do not include file modification times in serialized\n"+
		"                 ; directory data\n"+
		"  -http-server:<name> <url> ; PUT files to a N2R server.\n" +
		"  -flaky-http-server:<name> <url> ; Same, but retry a few times on server errors.\n" +
		"  -command-server:<name> <cmd> <arg> <arg> ... ';' ; Add a command to pipe\n" +
		"                 ; cmd-server commands to\n" +
		//"  -server-name <name> ; name of repository you are uploading to;\n" +
		//"    this is used for tracking which files have already been uploaded where.\n" +
		//"  -server-command <cmd> <arg> <arg> ... -- ; Command to pipe cmd-server\n" +
		//"    commands to.\n" +
		"  -debug         ; spew inner thoughts all over your terminal.\n" +
		"  -enable-fast-exit ; enable fast exiting when no data needs to be sent\n" +
		"                 ; (hangs on some systems).\n" +
		"  -show-progress ; show progress information on stderr.\n" +
		"\n" +
		"Note that commit info (besides author) must be given separately for each file\n" +
		"listed that is to have a commit created for it, and commit information must\n" +
		"be given *before* the path to the file/directory to be uploaded\n" +
		"\n" +
		"Local repository names are used as the first path component of heads.\n" +
		"Remote repository names are used to keep track of what files have already\n" +
		"been uploaded where.\n" +
		"\n" +
		"You can specify more than one remote server, and files will be uploaded to\n" +
		"all of them.\n" +
		"\n" +
		"Example usage:\n" +
		"  ccouch3 upload \\\n" +
		"    -command-server:example.org ssh tom@example.org \"ccouch3 cmd-server\" ';' \\\n" +
		"    -local-repo:tom /home/tom/ccouch \\\n" +
		"    -a Tom -n archives/tom/pictures -m \"Tom's pictures\" /home/tom/pics/ \\\n" +
		"    -a Tom -n archives/tom/docs -m \"Tom's documents\" /home/tom/docs/";
	
	public static int uploadMain( CCouchContext ctx, Iterator<String> args ) throws Exception {
		FlowUploaderCommand fuc = fromArgs(ctx, args, true, false);
		switch( fuc.mode ) {
		case ERROR:
			System.err.println( "Error: " + fuc.errorMessage + "\n" );
			System.err.println( UPLOAD_USAGE );
			return 1;
		case HELP:
			System.out.println( UPLOAD_USAGE );
			return 0;
		default:
			return new FlowUploader(fuc.config).runUpload();
		}
	}
	
	static final String IDENTIFY_USAGE =
		"Usage: ccouch3 id <file/dir> ...\n" +
		"\n" +
		"Identify files and directories without copying them anywhere.\n" +
		"\n" +
		"Options:\n" +
		"  -repo <path> ; Path to local ccouch repository to store cache in.\n" +
		"  -no-cache    ; Do not cache file hashes or upload records.\n" +
		"  -omit-file-mtimes ; do not include file modification times in\n"+
		"               ; serialized directory data\n"+
		"\n" +
		"Example usage:\n" +
		"  ccouch3 id something.txt some-directory/ somethingelse.zip";
	
	public static int identifyMain( CCouchContext ctx, Iterator<String> args ) throws Exception {
		FlowUploaderCommand fuc = fromArgs(ctx, args, false, true);
		switch( fuc.mode ) {
		case ERROR:
			System.err.println( "Error: " + fuc.errorMessage + "\n" );
			System.err.println( IDENTIFY_USAGE );
			return 1;
		case HELP:
			System.out.println( IDENTIFY_USAGE );
			return 0;
		default:
			new FlowUploader(fuc.config).runIdentify();
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(uploadMain( new CCouchContext(), Arrays.asList(args).iterator() ));
	}
}
