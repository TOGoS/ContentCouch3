package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.hash.BitprintDigest;

public class FlowUploader
{
	interface MessageDigestFactory {
		public MessageDigest createMessageDigest();
	}
	
	interface HashFormatter {
		public String format( byte[] hash );
	}

	interface Digestor {
		public String digest( InputStream is ) throws IOException;
	}

	static class MessageDigestor implements Digestor {
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
	
	////
	
	Collection<UploadTask> tasks;
	
	public FlowUploader( Collection<UploadTask> tasks ) {
		this.tasks = tasks;
	}
	
	static abstract class QueueRunner extends Thread {
		BlockingQueue<Object> inQueue;
		public QueueRunner( BlockingQueue<Object> inQueue, String name ) {
			super(name);
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
	
	static final String[] IGNORE_FILENAMES = {
		"thumbs.db", "desktop.ini"
	};
	
	interface Sink<E> {
		public void give( E value ) throws Exception;
	}
	
	static class QueueSink<E> implements Sink<E> {
		protected final BlockingQueue<E> q;
		public QueueSink( BlockingQueue<E> q ) { this.q = q; }
		public void give(E value) throws Exception { q.put(value); }
	}
	
	class HeadRequestSender implements Sink<Object> {
		protected final CmdWriter w;
		protected final Sink<Object> shortCut;
		
		boolean sentAnything = false;
		
		public HeadRequestSender( CmdWriter w, Sink<Object> shortCut ) {
			this.w = w;
			this.shortCut = shortCut;
		}
		
		protected void bye() throws Exception {
			if( !sentAnything ) shortCut.give(EndMessage.INSTANCE);
			w.bye();
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
			} else if( m instanceof FullyStoredMarker ) {
				FullyStoredMarker fsm = (FullyStoredMarker)m;
				w.writeCmd( new String[]{ "echo", "fully-stored", fsm.urn} );
			} else if( m instanceof EndMessage ) {
				bye();
			} else {
				bye();
				throw new RuntimeException("Don't know what to do with "+m.getClass());
			}
			sentAnything = true;
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
							messageSink.give( new FullyStoredMarker(m[2]) );
						} else if( "bye".equals(m[1]) ) {
							// Goodbye!
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
	
	static class Indexer
	{
		protected final DirectorySerializer dirSer;
		protected final Digestor digestor;
		protected final Sink<Object> indexResultSink;
		
		public Indexer( DirectorySerializer dirSer, Digestor digestor, Sink<Object> indexedFileInfoSink ) {
			this.dirSer = dirSer;
			this.digestor = digestor;
			this.indexResultSink = indexedFileInfoSink;
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
			// TODO: If a file or directory is fully stored, skip it!
			// TODO: Skip even hashing files or directories if they can be identified as a URN that is already fully stored.
			//   Don't try to skip ones where the hashes are known but they are not fully stored, because we need to pass
			//   those on to the uploader.
			
			FileInfo fi;
			if( file.isFile() ) {
				FileInputStream fis = new FileInputStream( file );
				try {
					fi = new FileInfo(
						file.getCanonicalPath(),
						digestor.digest(fis),
						FileInfo.FILETYPE_BLOB,
						file.length(),
						file.lastModified()
					);
				} finally {
					fis.close();
				}
				indexResultSink.give( fi );
			} else if( file.isDirectory() ) {
				Collection<DirectoryEntry> entries = indexDirectoryEntries( file );
				
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
	
	class Uploader implements Sink<Object> {
		protected final CmdWriter w;
		public Uploader( CmdWriter w ) { this.w = w; }
		
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
				}
				w.endChunks();
			} else if( m instanceof FullyStoredMarker ) {
				// TODO: Mark the stuff as stored!
			} else if( m instanceof EndMessage ) {
				w.bye();
			} else {
				throw new RuntimeException("Unexpected message: "+m.toString());
			}
		}
	}
	
	public void run() {
		String[] command = new String[]{ "java", "-cp", "bin", "togos.ccouch3.CmdServer", "-repo", "server-repo" };
		Process headProc, uploadProc;
		try {
			headProc = Runtime.getRuntime().exec(command);
			uploadProc = Runtime.getRuntime().exec(command);
		} catch( IOException e ) {
			throw new RuntimeException("Couldn't run cmdserver via exec");
		}
		final Uploader uploader = new Uploader( new CmdWriter(uploadProc.getOutputStream()) );
		final HeadRequestSender headRequestor = new HeadRequestSender( new CmdWriter(headProc.getOutputStream()), uploader );
		final CmdResponseReader headResponseReader = new CmdResponseReader(
			new CmdReader(headProc.getInputStream()),
			uploader
		);
		final CmdResponseReader uploadResponseReader = new CmdResponseReader(
			new CmdReader(uploadProc.getInputStream()),
			new Sink<Object>() { public void give(Object value) throws Exception {} }
		);
		/*
		headRequestor.w.debugPrefix = "Write to head proc: ";
		headResponseReader.r.debugPrefix = "Read from head proc: ";
		uploader.w.debugPrefix = "Write to upload proc: ";
		uploadResponseReader.r.debugPrefix = "Read from upload proc: ";
		*/
		
		final LinkedBlockingQueue<Object> uploadTaskQueue     = new LinkedBlockingQueue<Object>(tasks);
		final LinkedBlockingQueue<Object> logMessageQueue     = new LinkedBlockingQueue<Object>();
		
		final Digestor digestor = new MessageDigestor(new MessageDigestFactory() {
			@Override
			public MessageDigest createMessageDigest() {
				return new BitprintDigest();
			}
		}, new HashFormatter() {
			@Override
			public String format(byte[] hash) {
				return "urn:bitprint:" + BitprintDigest.format(hash);
			}
		});
		
		final DirectorySerializer dirSer = new NewStyleRDFDirectorySerializer();
		final Indexer indexer = new Indexer( dirSer, digestor, new Sink<Object>() {
			@Override
			public void give(Object m) throws Exception {
				headRequestor.give(m);
			}
		});
		
		final Thread indexThread = new QueueRunner( uploadTaskQueue, "Indexer" ) {			
			public boolean handleMessage( Object m ) throws Exception {
				if( m instanceof UploadTask ) {
					UploadTask ut = (UploadTask)m;
					FileInfo fi = indexer.index(ut.path);
					logMessageQueue.put(new DirectoryEntry( ut.name, fi ));
					return true;
				} else if( m instanceof EndMessage ) {
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
			
			@Override
			protected void cleanUp() throws Exception {
				headRequestor.give( EndMessage.INSTANCE );
				logMessageQueue.add( EndMessage.INSTANCE );
			}
		};
		final Thread headResponseReaderThread = new Thread( headResponseReader, "Head Response Reader" );
		final Thread uploadResponseReaderThread = new Thread( uploadResponseReader, "Upload Response Reader" );
		final Thread logThread   = new QueueRunner( logMessageQueue, "Log Writer" ) {
			@Override
			protected boolean handleMessage(Object m) throws Exception {
				if( m instanceof DirectoryEntry ) {
					DirectoryEntry de = (DirectoryEntry)m;
					System.err.println("Indexed root " + de.name + " = " + de.urn);
					return true;
				} else if( m instanceof EndMessage ) {
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
			
			@Override
			protected void cleanUp() throws Exception {}
		};
		
		indexThread.start();
		headResponseReaderThread.start();
		uploadResponseReaderThread.start();
		logThread.start();
		
		uploadTaskQueue.add( EndMessage.INSTANCE );
		
		try {
			indexThread.join();
			headResponseReaderThread.join();
			uploadResponseReaderThread.join();
			logThread.join();
		} catch( InterruptedException e ) {
			indexThread.interrupt();
			headResponseReaderThread.interrupt();
			uploadResponseReaderThread.interrupt();
			logThread.interrupt();
			Thread.currentThread().interrupt();
		}
	}
	
	public static void main( String[] args ) throws Exception {
		ArrayList<UploadTask> tasks = new ArrayList<UploadTask>();
		for( String a : args ) {
			tasks.add( new UploadTask(a, a) );
		}
		FlowUploader fu = new FlowUploader(tasks);
		fu.run();
	}
}
