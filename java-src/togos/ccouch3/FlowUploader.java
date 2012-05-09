package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.FlowUploader.StandardTransferTracker.Counter;
import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.hash.BitprintHashURNFormatter;
import togos.ccouch3.hash.HashFormatter;
import togos.ccouch3.slf.RandomAccessFileBlob;
import togos.ccouch3.slf.SimpleListFile2;

public class FlowUploader
{
	//// Hash stuff ////
	
	interface MessageDigestFactory {
		public MessageDigest createMessageDigest();
	}
	
	interface StreamURNifier {
		public String digest( InputStream is ) throws IOException;
	}

	static class MessageDigestor implements StreamURNifier {
		final MessageDigestFactory messageDigestFactory;
		final HashFormatter hform;
		
		public MessageDigestor( MessageDigestFactory fac, HashFormatter form ) {
			this.messageDigestFactory = fac;
			this.hform = form;
		}
		
		@Override
		public String digest(InputStream is) throws IOException {
			MessageDigest d = messageDigestFactory.createMessageDigest();
			
			byte[] buffer = new byte[65536];
			while( true ) {
				int z = is.read( buffer );
				if( z <= 0 ) break;
				d.update( buffer, 0, z );
					
			}
			
			return hform.format( d.digest() );
		}
	}
	
	static class BitprintMessageDigestFactory implements MessageDigestFactory {
		@Override
		public MessageDigest createMessageDigest() {
			return new BitprintDigest();
		}
	}
	
	public static final BitprintMessageDigestFactory BITPRINT_MESSAGE_DIGEST_FACTORY = new BitprintMessageDigestFactory(); 
	public static final MessageDigestor BITPRINT_STREAM_URNIFIER = new MessageDigestor( BITPRINT_MESSAGE_DIGEST_FACTORY, BitprintHashURNFormatter.INSTANCE );
	
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
		// Units = e.g. files, or whatever you want to track.
		public void transferred( long byteCount, int unitCount, String tag );
	}
	
	static class StandardTransferTracker implements TransferTracker {
		class Counter {
			long byteCount = 0;
			long unitCount = 0;
		}
		
		HashMap<String,Counter> counters = new HashMap<String,Counter>();
		protected Counter getCounter( String name ) {
			Counter c = counters.get(name);
			if( c == null ) counters.put( name, c = new Counter() );
			return c;
		}
		
		@Override
		public synchronized void transferred(long byteCount, int unitCount, String tag) {
			Counter c = getCounter(tag);
			c.byteCount += byteCount;
			c.unitCount += unitCount;
			
		}
	}
	
	//// Message types ////
	
	static class UploadTask {
		public final String name;
		public final String path;
		
		public UploadTask( String name, String path ) {
			this.name = name;
			this.path = path;
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
	
	static class BlobInfo {
		public final String urn;
		public final byte[] blob;
		
		public BlobInfo( String urn, byte[] blob ) {
			this.urn = urn;
			this.blob = blob;
		}
	}
	
	static class FullyStoredMarker {
		public final String urn;
		public FullyStoredMarker( String urn ) { this.urn = urn; }
	}
	
	static class FileMissing {
		public final String path;
		public final String urn;
		
		public FileMissing( String path, String urn ) {
			this.path = path;
			this.urn = urn;
		}
		
		@Override
		public String toString() {
			return "FileMissing path="+path+", urn="+urn;
		}
	}
	
	////
	
	static final ByteChunk YES_MARKER = BlobUtil.byteChunk("Y");
	
	protected static void mkParentDirs( File f ) {
		File p = f.getParentFile();
		if( p != null && !p.exists() ) p.mkdirs();
	}
	
	protected static SimpleListFile2 mkSlf( File f ) {
		mkParentDirs(f);
		try {
			return new SimpleListFile2( new RandomAccessFileBlob(f, "rw"), 16, true );
		} catch( FileNotFoundException e ) {
			// This should not happen!
			throw new RuntimeException(e);
		}
	}
		
	static final String[] IGNORE_FILENAMES = {
		"thumbs.db", "desktop.ini"
	};
	
	// TODO: split into URN cache and upload cache; they are sometimes needed independently
	
	interface UploadCache {
		public String getFileUrn( File f ) throws Exception;
		public void cacheFileUrn( File f, String urn ) throws Exception;
		public boolean isFullyUploaded( String urn ) throws Exception;
		public void markFullyUploaded( String urn ) throws Exception;
	}
	
	class SLFUploadCache implements UploadCache {
		final File fileUrnCacheFile;
		final File dirUrnCacheFile;
		final File uploadedCacheFile;
		
		SimpleListFile2 fileUrnCache;
		SimpleListFile2 dirUrnCache;
		SimpleListFile2 uploadedCache;
		
		public SLFUploadCache( File cacheDir, String serverName ) {
			this.fileUrnCacheFile = new File(cacheDir + "/file-urns.slf2");
			this.dirUrnCacheFile = new File(cacheDir + "/dir-urns.slf2");
			this.uploadedCacheFile = new File(cacheDir + "/uploaded-to-"+serverName+".slf2");
		}
		
		protected synchronized SimpleListFile2 getFileUrnCache() {
			if( fileUrnCache == null ) {
				fileUrnCache = mkSlf(fileUrnCacheFile);
			}
			return fileUrnCache;
		}
		
		protected synchronized SimpleListFile2 getDirUrnCache() {
			if( dirUrnCache == null ) {
				dirUrnCache = mkSlf(dirUrnCacheFile);
			}
			return dirUrnCache;
		}
		
		protected SimpleListFile2 getUrnCache( File f ) {
			return f.isDirectory() ? getDirUrnCache() : getFileUrnCache(); 
		}

		protected synchronized SimpleListFile2 getUploadedCache() {
			if( uploadedCache == null ) {
				uploadedCache = mkSlf(uploadedCacheFile);
			}
			return uploadedCache;
		}
		
		protected ByteChunk fileIdChunk( File f ) throws IOException {
			String fileId = f.getCanonicalPath() + ";mtime=" + f.lastModified() + ";size=" + f.length();
			return BlobUtil.byteChunk(fileId);
		}
		
		@Override
		public void cacheFileUrn(File f, String urn) throws IOException {
			SimpleListFile2 c = getUrnCache(f);
			ByteChunk fileIdChunk = fileIdChunk(f);
			ByteChunk urnChunk = BlobUtil.byteChunk(urn);
			synchronized( c ) { c.put( fileIdChunk, urnChunk ); }
		}
		
		@Override
		public String getFileUrn(File f) throws Exception {
			SimpleListFile2 c = getUrnCache(f);
			ByteChunk fileIdChunk = fileIdChunk(f);
			ByteChunk urnChunk;
			synchronized( c ) { urnChunk = c.get( fileIdChunk ); }
			return urnChunk != null ? BlobUtil.string( urnChunk ) : null;
		}
		
		@Override
		public void markFullyUploaded(String urn) throws Exception {
			SimpleListFile2 c = getUploadedCache();
			ByteChunk urnChunk = BlobUtil.byteChunk(urn);
			synchronized( c ) { c.put(urnChunk, YES_MARKER); }
		}
		
		@Override
		public boolean isFullyUploaded(String urn) throws Exception {
			SimpleListFile2 c = getUploadedCache();
			ByteChunk urnChunk = BlobUtil.byteChunk(urn);
			ByteChunk storedMarker;
			synchronized( c ) { storedMarker = c.get(urnChunk); }
			return YES_MARKER.equals(storedMarker);
		}
	}
	
	static class Indexer
	{
		protected final DirectorySerializer dirSer;
		protected final StreamURNifier digestor;
		protected final Sink<Object> indexResultSink;
		protected final UploadCache uploadCache;
		
		public Indexer( DirectorySerializer dirSer, StreamURNifier digestor, Sink<Object> indexedFileInfoSink, UploadCache uploadCache ) {
			this.dirSer = dirSer;
			this.digestor = digestor;
			this.indexResultSink = indexedFileInfoSink;
			this.uploadCache = uploadCache;
		}
		
		protected boolean shouldIgnore( File f ) {
			if( f.isHidden() || f.getName().startsWith(".") ) return true;
			String name = f.getName().toLowerCase();
			for( String ifn : IGNORE_FILENAMES ) if( ifn.equals(name) ) return true;
			return false;
		}
		
		public Collection<DirectoryEntry> indexDirectoryEntries( File file ) throws Exception {
			ArrayList<DirectoryEntry> entries = new ArrayList<DirectoryEntry>();
			
			for( File c : file.listFiles() ) {
				if( !shouldIgnore(c) ) entries.add( new DirectoryEntry( c.getName(), index(c) ) );
			}
			
			return entries;
		}
		
		protected FileInfo index( File file ) throws Exception {
			String cachedUrn = uploadCache.getFileUrn( file );
			if( cachedUrn != null && uploadCache.isFullyUploaded(cachedUrn) ) {
				return new FileInfo(
					file.getCanonicalPath(),
					cachedUrn,
					file.isDirectory() ? FileInfo.FILETYPE_DIRECTORY : FileInfo.FILETYPE_BLOB,
					file.length(),
					file.lastModified()
				);
			}
			
			FileInfo fi;
			if( file.isFile() ) {
				String fileUrn;
				if( cachedUrn != null ) {
					fileUrn = cachedUrn;
				} else {
					FileInputStream fis = new FileInputStream( file );
					try {
						fileUrn = digestor.digest(fis);
					} finally {
						fis.close();
					}
				}
				
				fi = new FileInfo(
					file.getCanonicalPath(),
					fileUrn,
					FileInfo.FILETYPE_BLOB,
					file.length(),
					file.lastModified()
				);
				
				uploadCache.cacheFileUrn( file, fileUrn );
				
				indexResultSink.give( fi );
			} else if( file.isDirectory() ) {
				// Even if we already know the URN, we still need to walk the entire
				// directory to push all non-fully-uploaded subfiles/directories to the uploader.
				Collection<DirectoryEntry> entries = indexDirectoryEntries( file );
				
				// Will become true if it has subdirectories *that we want to index*.
				// If so, we *cannot* rely on the hash cache, and will not store it.
				boolean includesSubDirs = false;
				for( DirectoryEntry e : entries ) {
					if( e.fileType == DirectoryEntry.FILETYPE_DIRECTORY ) {
						includesSubDirs = true;
					}
				}
				
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				dirSer.serialize(entries, baos);
				baos.close();
				
				String rdfBlobUrn = digestor.digest(new ByteArrayInputStream(baos.toByteArray()));
				String treeUrn = "x-rdf-subject:" + rdfBlobUrn;
				
				fi = new FileInfo(
					file.getCanonicalPath(),
					treeUrn,
					FileInfo.FILETYPE_DIRECTORY,
					file.length(),
					file.lastModified()
				);
				
				if( !includesSubDirs && treeUrn != cachedUrn ) {
					uploadCache.cacheFileUrn( file, treeUrn );
				}
				if( uploadCache.isFullyUploaded(treeUrn) ) {
					return fi;
				}
				
				BlobInfo blobInfo = new BlobInfo( rdfBlobUrn, baos.toByteArray() );
				indexResultSink.give( blobInfo );
				indexResultSink.give( new FullyStoredMarker(treeUrn) );
			} else {
				throw new RuntimeException("Don't know how to index "+file);
			}
			return fi;
		}
		
		protected FileInfo index( String path ) throws Exception {
			return index( new File(path) );
		}
	}
	
	class HeadRequestSender implements Sink<Object> {
		protected final CmdWriter w;
		protected final TransferTracker tt;
		
		public HeadRequestSender( CmdWriter w, TransferTracker tt ) {
			this.w = w;
			this.tt = tt;
		}
		
		@Override
		public void give( Object m ) throws Exception {
			if( m instanceof FileInfo ) {
				FileInfo fi = (FileInfo)m;
				w.writeCmd( new String[]{ "head", fi.path, fi.urn } );
			} else if( m instanceof BlobInfo ) {
				BlobInfo bi = (BlobInfo)m;
				w.writeCmd( new String[]{ "put", bi.urn, bi.urn, "chunk", String.valueOf(bi.blob.length) } );
				w.writeChunk( bi.blob, 0, bi.blob.length );
				w.endChunks();
				tt.transferred( bi.blob.length, 1, "treenode" );
			} else if( m instanceof FullyStoredMarker ) {
				FullyStoredMarker fsm = (FullyStoredMarker)m;
				w.writeCmd( new String[]{ "echo", "fully-stored", fsm.urn} );
			} else if( m instanceof EndMessage ) {
				w.bye();
			} else {
				w.bye();
				throw new RuntimeException("Don't know what to do with "+m.getClass());
			}
		}
	}
	
	class CmdResponseReader implements Runnable {
		protected final CmdReader r;
		protected final Sink<Object> messageSink;
		
		public CmdResponseReader( CmdReader r, Sink<Object> messageSink ) {
			this.r = r;
			this.messageSink = messageSink;
		}
		
		public void run() {
			String[] m;
			try {
				while( (m = r.readCmd()) != null ) {
					String responseType = m[0];
					if( "error".equals(responseType) ) {
						throw new RuntimeException("Error from server: "+CmdWriter.encode(m));
					} else if( "ok".equals(responseType) ) {
						if( "head".equals(m[1]) ) {
							// 0:ok 1:head 2:<path> 3:<urn> 4:{found|missing}
							boolean found = "found".equals(m[4]);
							if( found ) {
								messageSink.give( new FullyStoredMarker(m[3]) );
							} else {
								messageSink.give( new FileMissing(m[2], m[3]) );
							}
						} else if( "put".equals(m[1]) && m.length >= 3 ) {
							// 0:ok 1:put 2:<urn> 3:<urn>
							// Then one of our blobs went through; woot.
							messageSink.give( new FullyStoredMarker(m[3]) );
						} else if( "echo".equals(m[1]) && m.length == 4 && "fully-stored".equals(m[2]) ) {
							// 0:ok 1:echo 2:fully-stored 3:<urn>
							messageSink.give( new FullyStoredMarker(m[3]) );
						} else if( "bye".equals(m[1]) ) {
							// Goodbye!
							messageSink.give( EndMessage.INSTANCE );
						} else {
							throw new RuntimeException("Unexpected result line from server: "+CmdWriter.encode(m));
						}
					} else {
						throw new RuntimeException("Unexpected message from server: "+CmdWriter.encode(m));
					}
				}
			} catch( Exception e ) {
				throw new RuntimeException(e);
			} finally {
				try { r.close(); } catch( IOException e ) {}
				try { messageSink.give( EndMessage.INSTANCE ); } catch( Exception e ) {}
			}
		}
	}
	
	class Uploader implements Sink<Object> {
		protected final CmdWriter w;
		protected final TransferTracker tt;
		public Uploader( CmdWriter w, TransferTracker tt ) {
			this.w = w;
			this.tt = tt;
		}
		
		public int uploadCount = 0;
		
		@Override
		public void give(Object m) throws Exception {
			if( m instanceof FileMissing ) {
				FileMissing fm = (FileMissing)m;
				File f = new File(fm.path);
				w.writeCmd( new String[] { "put", fm.urn, fm.urn, "chunk", String.valueOf(f.length()) } );
				byte[] buffer = new byte[(int)Math.min(1024*1024, f.length())];
				FileInputStream fis = new FileInputStream(f);
				for( int z = fis.read(buffer); z >= 0; z = fis.read(buffer) ) {
					w.writeChunk( buffer, 0, z );
					tt.transferred( z, 0, "file" );
				}
				w.endChunks();
				tt.transferred( 0, 1, "file" );
			} else if( m instanceof FullyStoredMarker ) {
				FullyStoredMarker fsm = (FullyStoredMarker)m;
				w.writeCmd( new String[]{ "echo", "fully-stored", fsm.urn} );
			} else if( m instanceof EndMessage ) {
				w.bye();
			} else {
				w.bye();
				throw new RuntimeException("Unexpected message: "+m.toString());
			}
		}
	}
	
	//// Put it all together ////
	
	Collection<UploadTask> tasks;
	public boolean showTransferSummary;
	public boolean reportPathUrnMapping = false;
	public boolean reportUrn = false;
	public StreamURNifier digestor = BITPRINT_STREAM_URNIFIER;
	public DirectorySerializer dirSer = new NewStyleRDFDirectorySerializer();
	public File cacheDir;
	public String serverName;
	public String[] serverCommand; // = new String[]{ "java", "-cp", "bin", "togos.ccouch3.CmdServer", "-repo", ".server-repo" };
		
	public FlowUploader( Collection<UploadTask> tasks ) {
		this.tasks = tasks;
	}
	
	UploadCache uc;
	protected synchronized UploadCache getUploadCache() {
		if( cacheDir == null ) {
			return new UploadCache() {
				@Override public void markFullyUploaded(String urn) throws Exception {}
				@Override public boolean isFullyUploaded(String urn) throws Exception { return false; }
				@Override public String getFileUrn(File f) throws Exception { return null; }
				@Override public void cacheFileUrn(File f, String urn) throws Exception {}
			};
		}
		if( uc == null ) {
			uc = new SLFUploadCache(cacheDir, serverName);
		}
		return uc;
	}
	
	public void runIdentify() throws Exception {
		final Indexer indexer = new Indexer( dirSer, digestor, new Sink<Object>() {
			@Override
			public void give(Object m) throws Exception {}
		}, getUploadCache());
		for( UploadTask ut : tasks ) {
			FileInfo fi = indexer.index(ut.path);
			System.out.println( ut.name + "\t" + fi.urn );
		}
	}
	
	public void runStore() {
		final Process headProc, uploadProc;
		try {
			headProc = Runtime.getRuntime().exec(serverCommand);
			uploadProc = Runtime.getRuntime().exec(serverCommand);
		} catch( IOException e ) {
			throw new RuntimeException("Couldn't run cmdserver via exec");
		}
		final StandardTransferTracker tt = new StandardTransferTracker();
		final Uploader uploader = new Uploader( new CmdWriter(uploadProc.getOutputStream()), tt );
		final HeadRequestSender headRequestor = new HeadRequestSender( new CmdWriter(headProc.getOutputStream()), tt );
		final CmdResponseReader headResponseReader = new CmdResponseReader(
			new CmdReader(headProc.getInputStream()),
			uploader
		);
		final CmdResponseReader uploadResponseReader = new CmdResponseReader(
			new CmdReader(uploadProc.getInputStream()),
			new Sink<Object>() { public void give(Object value) throws Exception {
				if( value instanceof FullyStoredMarker ) {
					FullyStoredMarker fsm = (FullyStoredMarker)value;
					getUploadCache().markFullyUploaded( fsm.urn );
				}
			} }
		);
		/*
		headRequestor.w.debugPrefix = "Write to head proc: ";
		headResponseReader.r.debugPrefix = "Read from head proc: ";
		uploader.w.debugPrefix = "Write to upload proc: ";
		uploadResponseReader.r.debugPrefix = "Read from upload proc: ";
		*/
		
		final LinkedBlockingQueue<Object> uploadTaskQueue     = new LinkedBlockingQueue<Object>(tasks);
		final LinkedBlockingQueue<Object> indexOutput         = new LinkedBlockingQueue<Object>();
		
		final Indexer indexer = new Indexer( dirSer, digestor, new QueueSink<Object>(indexOutput), getUploadCache());
		final QueueRunner indexRunner = new QueueRunner( uploadTaskQueue ) {
			public boolean handleMessage( Object m ) throws Exception {
				if( m instanceof UploadTask ) {
					UploadTask ut = (UploadTask)m;
					FileInfo fi = indexer.index(ut.path);
					if( reportUrn ) {
						System.out.println(fi.urn);
					}
					if( reportPathUrnMapping ) {
						System.out.println(fi.path+"\t"+fi.urn);
					}
					return true;
				} else if( m instanceof EndMessage ) {
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
			
			@Override
			protected void cleanUp() throws Exception {
				indexOutput.put( EndMessage.INSTANCE );
			}
		};
		
		final Thread indexThread = new Thread( indexRunner, "Indexer" );
		final Thread headResponseReaderThread = new Thread( headResponseReader, "Head Response Reader" );
		final Thread uploadResponseReaderThread = new Thread( uploadResponseReader, "Upload Response Reader" );
		final Thread indexOutputThread = new Thread( new QueueRunner( indexOutput ) {
			boolean anythingSent;
			
			@Override
			protected void cleanUp() throws Exception {}
			
			@Override
			public boolean handleMessage(Object m) throws Exception {
				// If nothing's been sent before EndMessage, then there's nothing
				// for other processes to do and we can quit early!
				if( !anythingSent && m instanceof EndMessage ) {
					// Close the processes so that read threads will return immediately
					headProc.destroy();
					uploadProc.destroy();
				} else {
					anythingSent = true;
					headRequestor.give(m);
				}
				return !(m instanceof EndMessage);
			}
		}, "Index Output Filter" );
		
		indexThread.start();
		headResponseReaderThread.start();
		uploadResponseReaderThread.start();
		indexOutputThread.start();
		
		uploadTaskQueue.add( EndMessage.INSTANCE );
		
		try {
			indexThread.join();
			indexOutputThread.join();
			headResponseReaderThread.join();
			uploadResponseReaderThread.join();
		} catch( InterruptedException e ) {
			indexThread.interrupt();
			indexOutputThread.interrupt();
			headResponseReaderThread.interrupt();
			uploadResponseReaderThread.interrupt();
			Thread.currentThread().interrupt();
		}
		
		if( showTransferSummary ) {
			if( tt.counters.isEmpty() ) {
				System.err.println( "No transfers!" );
			}
			for( Map.Entry<String,Counter> thing : tt.counters.entrySet() ) {
				System.err.println( "Transferred "+thing.getValue().byteCount+" bytes in "+thing.getValue().unitCount+" "+thing.getKey()+"s");
			}
		}
	}
	
	static FlowUploader fromArgs( Iterator<String> args ) throws Exception {
		ArrayList<UploadTask> tasks = new ArrayList<UploadTask>();
		boolean verbose = false; // false!;
		String cacheDir = null;
		String serverName = null;
		String[] serverCommand = null;
		for( ; args.hasNext(); ) {
			String a = args.next();
			if( "-v".equals(a) ) {
				verbose = true;
			} else if( "-no-cache".equals(a) ) {
				cacheDir = "DO-NOT-CACHE";
			} else if( "-cache-dir".equals(a) ) {
				cacheDir = args.next();
			} else if( "-server-name".equals(a) ) {
				serverName = args.next();
			} else if( "-server-command".equals(a) ) {
				List<String> sc = new ArrayList<String>();
				for( a = args.next(); a != null && !"--".equals(a); a = args.next() ) {
					sc.add( a );
				}
				serverCommand = sc.toArray(new String[sc.size()]);
			} else if( !a.startsWith("-") ) {
				tasks.add( new UploadTask(a, a) );
			} else {
				System.err.println("Unrecognised argument: "+a);
				return null;
			}
		}
		
		FlowUploader fu = new FlowUploader(tasks);
		
		if( "DO-NOT-CACHE".equals(cacheDir) ) {
			fu.cacheDir = null;
		} else if( cacheDir == null ) {
			String homeDir = System.getProperty("user.home");
			if( homeDir == null ) homeDir = ".";
			fu.cacheDir = new File(homeDir + "/.ccouch/cache/flow-uploader");
		} else {
			fu.cacheDir = new File(cacheDir);
		}
		
		fu.showTransferSummary = verbose;
		fu.reportPathUrnMapping = verbose && tasks.size() != 1;
		fu.reportUrn = verbose && tasks.size() == 1;
		fu.serverName = serverName;
		fu.serverCommand = serverCommand;
		return fu;
	}
	
	public static int storeMain( Iterator<String> args ) throws Exception {
		FlowUploader fu = fromArgs(args);
		if( fu == null ) return 1;
		if( fu.serverCommand == null ) {
			System.err.println("No -server-command given.  Syntax: -server-command <cmd> <arg1> <arg2> ... --");
			return 1;
		}
		if( fu.serverName == null ) {
			System.err.println("No -server-name given.  Syntax: -server-name <name>");
			return 1;
		}
		fu.runStore();
		return 0;
	}
	
	public static int identifyMain( Iterator<String> args ) throws Exception {
		FlowUploader fu = fromArgs(args);
		if( fu == null ) return 1;
		fu.runIdentify();
		return 0;
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit(storeMain( Arrays.asList(args).iterator() ));
	}
}
