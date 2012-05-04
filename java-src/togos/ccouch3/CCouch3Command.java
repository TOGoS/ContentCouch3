package togos.ccouch3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import togos.ccouch3.hash.BitprintDigest;
import togos.ccouch3.util.LinkedList;

public class CCouch3Command
{
	class IdentifyResult {
		public final String path;
		public final String id;
		
		public IdentifyResult( String path, String id ) {
			this.path = path;
			this.id = id;
		}
	}
	
	public static final int FILETYPE_BLOB = 1;
	public static final int FILETYPE_DIR = 2;
	
	static class FileIdentifyResult implements Comparable<FileIdentifyResult> {
		public final File file;
		public final String id;
		public final long size;
		public final long mtime;
		public final int type;
		
		public FileIdentifyResult( File file, String id, long size, long mtime, int type ) {
			this.file = file;
			this.id = id;
			this.size = size;
			this.mtime = mtime;
			this.type = type;
		}
		
		@Override
		public int compareTo( FileIdentifyResult o ) {
			return file.compareTo(o.file);
		}
	}
	
	interface Digestor {
		public String digest( InputStream is ) throws IOException;
	}
	
	interface MessageDigestFactory {
		public MessageDigest createMessageDigest();
	}
	
	interface HashFormatter {
		public String format( byte[] hash );
	}
	
	class MessageDigestor implements Digestor {
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
	
	class FileIdentifier implements Callable<String> {
		final ExecutorService executor; 
		final File file;
		final Digestor digestor;
		
		public FileIdentifier( File f, Digestor d, ExecutorService executor ) {
			this.file = f;
			this.digestor = d;
			this.executor = executor;
		}
		
		@Override
		public String call() throws Exception {
			if( file.isFile() ) {
				System.err.println("Running blob identify in thread "+Thread.currentThread().getName());
				FileInputStream fis = new FileInputStream(file);
				try {
					return digestor.digest(fis);
				} finally {
					fis.close();
				}
			} else if( file.isDirectory() ) {
				LinkedList<Future<FileIdentifyResult>> idResultFutures = LinkedList.empty();
				for( File f : file.listFiles() ) {
					FutureTask<FileIdentifyResult> t = new FutureTask<FileIdentifyResult>(
						new FileIdentifyResultCreator( f,
							new FileIdentifier( f, digestor, executor )));
					executor.submit( t );
					idResultFutures = idResultFutures.prepend( t );
				}
				ArrayList<FileIdentifyResult> fileResults = new ArrayList<FileIdentifyResult>();
				for( ; idResultFutures.length > 0 ; idResultFutures = idResultFutures.rest ) {
					fileResults.add( idResultFutures.headValue.get() );
				}
				Collections.sort(fileResults);
				
				StringBuilder sb = new StringBuilder();
				for( FileIdentifyResult r : fileResults ) {
					sb.append( r.file.getName() + " " + r.type + " " + r.id + " " + r.mtime + " " + r.size + "\n" );
				}
				return "dir:" + digestor.digest( new ByteArrayInputStream( sb.toString().getBytes() ) );
			} else {
				throw new RuntimeException("Can't identify non-normal file: "+file);
			}
		}
	}
	
	class IdentifyResultCreator implements Callable<IdentifyResult> {
		public final String path;
		public final Callable<String> identifier;
		
		public IdentifyResultCreator( String path, Callable<String> identifier ) {
			this.path = path;
			this.identifier = identifier;
		}
		
		@Override
		public IdentifyResult call() throws Exception {
			return new IdentifyResult(path, identifier.call());
		}
	}
	
	class FileIdentifyResultCreator implements Callable<FileIdentifyResult> {
		public final File file;
		public final Callable<String> identifier;
		
		public FileIdentifyResultCreator( File file, Callable<String> identifier ) {
			this.file = file;
			this.identifier = identifier;
		}
		
		@Override
		public FileIdentifyResult call() throws Exception {
			return new FileIdentifyResult(file, identifier.call(), file.length(), file.lastModified(), file.isDirectory() ? FILETYPE_DIR : FILETYPE_BLOB );
		}
	}
	
	public int runStore( LinkedList<? extends String> args ) throws Exception {
		ExecutorService pool = Executors.newCachedThreadPool();
		LinkedList<String> paths = LinkedList.empty();
		
		while( args.length > 0 ) {
			if( !args.headValue.startsWith("-") ) {
				paths = new LinkedList<String>( args.headValue, paths );
			}
			args = args.rest;
		}
		
		LinkedList<Future<IdentifyResult>> identifyTasks = LinkedList.empty();
		Digestor digestor = new MessageDigestor(new MessageDigestFactory() {
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
		
		for( ; paths.length > 0 ; paths = paths.rest ) {
			FutureTask<IdentifyResult> t = new FutureTask<IdentifyResult>(
				new IdentifyResultCreator( paths.headValue,
					new FileIdentifier( new File(paths.headValue), digestor, pool )));
			pool.submit( t );
			identifyTasks = identifyTasks.prepend( t );
		}
		
		for( ; identifyTasks.length > 0 ; identifyTasks = identifyTasks.rest ) {
			IdentifyResult ir = identifyTasks.headValue.get();
			System.out.println( ir.path + "\t" + ir.id );
		}
		
		return 0;
	}
	
	public int run( LinkedList<String> args ) throws Exception {
		if( "store".equals(args.headValue) ) {
			return runStore(args.rest);
		} else {
			System.err.println("Unrecognised command: "+args.headValue);
			return 1;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		System.exit( new CCouch3Command().run( LinkedList.from(args) ) );
	}
}
