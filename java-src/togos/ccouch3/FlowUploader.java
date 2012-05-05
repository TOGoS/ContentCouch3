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
		public final String rootName;
		public final String path;
		
		public UploadTask( String rootName, String path ) {
			this.rootName = rootName;
			this.path = path;
		}
	}
	
	Collection<UploadTask> tasks;
	
	public FlowUploader( Collection<UploadTask> tasks ) {
		this.tasks = tasks;
	}
	
	/** Marks the end of a stream of messages, usually indicating that the recipient can quit. */
	static class EndMessage {
		public static final EndMessage INSTANCE = new EndMessage();
		private EndMessage() {}
	}
	
	static abstract class QueueRunner extends Thread {
		BlockingQueue<Object> inQueue;
		public QueueRunner( BlockingQueue<Object> inQueue ) {
			this.inQueue = inQueue;
		}
		protected abstract boolean handleMessage( Object m ) throws Exception;
		public void run() {
			Object m;
			try {
				while( (m = inQueue.take()) != null && handleMessage(m) );
			} catch( InterruptedException e ) {
				Thread.currentThread().interrupt();
			} catch( Exception e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	static final String[] IGNORE_FILENAMES = {
		"thumbs.db", "desktop.ini"
	};
	
	interface Sink<E> {
		public void give( E value ) throws Exception;
	}
	
	static class Indexer
	{
		protected final DirectorySerializer dirSer;
		protected final Digestor digestor;
		protected final Sink<FileInfo> indexedFileInfoSink;
		
		public Indexer( DirectorySerializer dirSer, Digestor digestor, Sink<FileInfo> indexedFileInfoSink ) {
			this.dirSer = dirSer;
			this.digestor = digestor;
			this.indexedFileInfoSink = indexedFileInfoSink;
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
			} else if( file.isDirectory() ) {
				Collection<DirectoryEntry> entries = indexDirectoryEntries( file );
				
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				dirSer.serialize(entries, baos);
				baos.close();
				
				fi = new FileInfo(
					file.getCanonicalPath(),
					"x-rdf-subject:" + digestor.digest(new ByteArrayInputStream(baos.toByteArray())),
					FileInfo.FILETYPE_DIRECTORY,
					file.length(),
					file.lastModified()
				);
			} else {
				throw new RuntimeException("Don't know how to index "+file);
			}
			indexedFileInfoSink.give( fi );
			return fi;
		}
		
		protected FileInfo index( String path ) throws Exception {
			return index( new File(path) );
		}
	}
	
	public void run() {
		final LinkedBlockingQueue<Object> uploadTaskQueue     = new LinkedBlockingQueue<Object>(tasks);
		final LinkedBlockingQueue<Object> filterMessageQueue  = new LinkedBlockingQueue<Object>();
		final LinkedBlockingQueue<Object> uploadMessageQueue  = new LinkedBlockingQueue<Object>();
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
		final Indexer indexer = new Indexer( dirSer, digestor, new Sink<FileInfo>() {
			@Override
			public void give(FileInfo value) throws InterruptedException {
				filterMessageQueue.put(value);
			}
		});
		
		Thread indexThread = new QueueRunner( uploadTaskQueue ) {			
			public boolean handleMessage( Object m ) throws Exception {
				if( m instanceof UploadTask ) {
					UploadTask ut = (UploadTask)m;
					logMessageQueue.put(new DirectoryEntry( ut.rootName, indexer.index(ut.path) ));
					return true;
				} else if( m == EndMessage.INSTANCE ) {
					filterMessageQueue.add( m );
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
		};
		Thread filterthread = new QueueRunner( filterMessageQueue ) {
			@Override
			protected boolean handleMessage(Object m) throws Exception {
				if( m instanceof FileInfo ) {
					FileInfo im = (FileInfo)m;
					System.err.println( "Indexed "+im.path+" = "+im.urn);
					return true;
				} else if( m == EndMessage.INSTANCE ) {
					uploadMessageQueue.add( m );
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
		};
		Thread uploadThread = new Thread();
		Thread logThread   = new QueueRunner( logMessageQueue ) {
			@Override
			protected boolean handleMessage(Object m) throws Exception {
				if( m instanceof DirectoryEntry ) {
					DirectoryEntry de = (DirectoryEntry)m;
					System.err.println("Indexed root " + de.name + " = " + de.urn);
					return true;
				} else if( m == EndMessage.INSTANCE ) {
					return false;
				} else {
					throw new RuntimeException("Unrecognised message type "+m.getClass());
				}
			}
		};
		
		indexThread.start();
		filterthread.start();
		uploadThread.start();
		logThread.start();
		
		uploadTaskQueue.add( EndMessage.INSTANCE );
		
		try {
			indexThread.join();
			filterthread.join();
			uploadThread.join();
			logThread.join();
		} catch( InterruptedException e ) {
			indexThread.interrupt();
			filterthread.interrupt();
			uploadThread.interrupt();
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
