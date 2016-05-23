package togos.ccouch3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import togos.ccouch3.FlowUploader.EndMessage;
import togos.ccouch3.FlowUploader.FullyStoredMarker;
import togos.ccouch3.FlowUploader.LogMessage;
import togos.ccouch3.FlowUploader.PutHead;
import togos.ccouch3.FlowUploader.TransferTracker;
import togos.ccouch3.util.AddableSet;
import togos.ccouch3.util.StreamUtil;

class HTTPUploadClient implements UploadClient
{
	static abstract class WorkerThread extends Thread {
		protected boolean completed = false;
		protected boolean anyFailures = false;
		
		public WorkerThread( String name ) {
			super(name);
		}
		
		public boolean completedSuccessfully() {
			return completed && !anyFailures;
		}
	}
	
	protected final String serverName;
	protected final String serverUrl;
	protected final AddableSet<String> fullyCachedUrnSet;
	protected final TransferTracker transferTracker;
	protected final Logger logger;
	
	protected final ArrayBlockingQueue<Object> headTasks = new ArrayBlockingQueue<Object>(1024);
	protected final ArrayBlockingQueue<Object> putTasks = new ArrayBlockingQueue<Object>(1024);
	
	public boolean debug = false;
	protected final WorkerThread headThread, putThread;
	
	// To test error handling, set these to true:
	boolean induceHeadErrors = false;
	boolean inducePutErrors = false;
	
	public HTTPUploadClient(
		String serverName, String serverUrl, AddableSet<String> uc, TransferTracker tt, Logger logger
	) {
		this.serverName = serverName;
		this.serverUrl = serverUrl;
		this.fullyCachedUrnSet = uc;
		this.transferTracker = tt;
		this.logger = logger;
		
		this.headThread = new HeadThread(serverName+" HEAD thread");
		this.putThread  = new PutThread(serverName+" PUT thread");
	}
	
	@Override public String getServerName() { return serverName; }
	
	protected URL urlFor(String urn) throws MalformedURLException {
		return new URL(serverUrl + urn);
	}
	
	protected boolean existsOnServer( String urn ) throws IOException {
		URL url = urlFor(urn);
		
		if( induceHeadErrors ) throw new ServerError("Not a real error ha ha h", 599, null, "HEAD", url.toString());
		
		HttpURLConnection urlCon = (HttpURLConnection)url.openConnection();
		urlCon.setRequestMethod("HEAD");
		urlCon.connect();
		int status = urlCon.getResponseCode();
		if( debug ) System.err.println("HEAD "+url+" -> "+status);
		if( status == 200 ) {
			return true;
		} else if( status == 404 ) {
			return false;
		} else {
			byte[] errorText = null;
			try {
				errorText = StreamUtil.slurp(urlCon.getErrorStream());
			} catch( Exception e ) { }
			
			throw new ServerError(
				"HEAD received unexpected response code "+status+"; "+urlCon.getResponseMessage(),
				status,
				errorText,
				urlCon.getRequestMethod(),
				url.toString()
			);
		}
	}
	
	static class ServerError extends IOException {
		private static final long serialVersionUID = 1L;
				
		public final int statusCode;
		public final byte[] data;
		public final String requestMethod;
		public final String requestUrl;
		
		public ServerError( String message, int statusCode, byte[] data, String requestMethod, String requestUrl ) {
			super(message);
			this.statusCode = statusCode;
			this.data = data;
			this.requestMethod = requestMethod;
			this.requestUrl = requestUrl;
		}
		
		@Override public String getMessage() {
			if( this.data != null ) {
				try {
					String text = new String(data, "UTF-8");
					return super.getMessage()+"\n\n-- Data --\n"+text;
				} catch( Exception e ) {
					return super.getMessage()+"\n(Error while converting response data to string)";
				}
			}
			return super.getMessage();
		}
	}
		
	protected void _upload( BlobInfo bi, String tag ) throws ServerError, IOException {
		if( bi instanceof FileInfo ) {
			transferTracker.sendingFile( ((FileInfo)bi).getPath() );
		}
		
		InputStream is = bi.openInputStream();
		try {
			URL url = urlFor(bi.getUrn());
			
			if( inducePutErrors ) {
				throw new ServerError("Not a real server error; yuk yuk.", 599, null, "PUT", url.toString());
			}
			
			HttpURLConnection urlCon = (HttpURLConnection)url.openConnection();
			urlCon.setRequestMethod("PUT");
			urlCon.setDoOutput(true);
			OutputStream os = urlCon.getOutputStream();
			int totalSize = 0;
			try {
				byte[] buf = new byte[1024*1024];
				int r;
				while( (r = is.read(buf)) > 0 ) {
					totalSize += r;
					os.write(buf, 0, r);
				}
			} finally {
				os.close();
			}
			int status = urlCon.getResponseCode();
			if( debug ) System.err.println("PUT "+url+" ("+totalSize+" bytes) -> "+status);
			if( status < 200 || status >= 300 ) {
				byte[] errorText = null;
				try {
					errorText = StreamUtil.slurp(urlCon.getErrorStream());
				} catch( Exception e ) { }
				
				throw new ServerError(
					"PUT received unexpected response code "+status+"; "+urlCon.getResponseMessage(),
					status, errorText,
					"PUT", url.toString()
				);
			}
		} finally {
			is.close();
		}
		transferTracker.transferred(bi.getSize(), 1, tag);
	}
	
	public int maxUploadAttempts = 1;
	
	protected void upload( BlobInfo bi, String tag ) throws IOException {
		int attemptsLeft = maxUploadAttempts;
		int waitBetweenAttempts = 1000;
		while( true ) {
			try {
				_upload( bi, tag );
				return;
			} catch( ServerError serverError ) {
				--attemptsLeft;
				if( attemptsLeft == 0 ) {
					throw serverError;
				}
				
				if( debug ) {
					System.err.println(serverError.getMessage());
					System.err.println("Will try again in "+waitBetweenAttempts/1000+" seconds");
				}
				try {
					Thread.sleep(waitBetweenAttempts);
				} catch( InterruptedException e1 ) {
					throw serverError;
				}
				waitBetweenAttempts *= 2;
			}
		}
	}
	
	protected void upload( BlobInfo bi ) throws IOException {
		upload( bi, bi instanceof FileInfo ? TransferTracker.TAG_FILE : TransferTracker.TAG_TREENODE );
	}
	
	protected void reportServerError( ServerError e, boolean isFailure ) {
		System.err.println("Server error"+(isFailure ? "" : " (but not necessarily a failure)")+" from "+e.requestMethod+" "+e.requestUrl);
		try {
			System.err.println("  status="+e.statusCode+"; see "+logger.dumpToLog(e.data)+" for details");
		} catch( IOException le ) {
			System.err.println("  status="+e.statusCode+"; failed to dump output to file ("+le.getMessage()+", so here it is:");
			try {
				System.err.write(e.data);
			} catch( IOException le2 ) {
				System.err.println("  Also failed to dump server output to STDERR.  I give up.");
			}
		}
	}
	
	final int SMALL_BLOB_SIZE = 8192;
	
	/** Drains queues that we've given up on so the input process doesn't block */
	static class QueueDrainer<T> extends Thread {
		BlockingQueue<T> q;
		public QueueDrainer( BlockingQueue<T> q ) {
			setDaemon(true);
			this.q = q;
		}
		@Override public void run() {
			try {
				while( !(q.take() instanceof EndMessage) );
			} catch( InterruptedException e ) {
				// Okay, that's enough, then.
				interrupt();
			}
		}
		public static <T> void drain( BlockingQueue<T> q ) {
			new QueueDrainer<T>(q).start();
		}
	}
	
	class HeadThread extends WorkerThread {
		public HeadThread( String name ) { super(name); }
		
		protected boolean endReceived = false;
		protected boolean keepGoing = true;
		
		/** Returns true to indicate that the message should be forwarded */
		protected boolean handle(Object m) throws Exception {
			if( debug ) System.err.println(getName()+" received "+m.getClass());
			if( m instanceof BlobInfo ) {
				// Is it small?  Then just upload it.
				BlobInfo bi = (BlobInfo)m;
				boolean putAttemptedAndFailed = false;
				if( bi.getSize() < SMALL_BLOB_SIZE ) {
					// Try uploading it!
					try {
						upload(bi);
						return false;
					} catch( ServerError e ) {
						// If that errors, report the error but otherwise continue as if we hadn't tried.
						// We might next find that it exists on the server, in which case everything's
						// fine.  But if it /doesn't/ then that was a failure.
						reportServerError(e, false);
						putAttemptedAndFailed = true;
					}
				}
				
				try {
					if( existsOnServer(bi.getUrn()) ) {
						return false;
					}
				} catch( IOException e ) {
					anyFailures = true;
					System.err.println("Failed to determine if "+bi.getUrn()+" exists on "+serverUrl+" due to error: "+e.getMessage());
					return false;
				}
				
				if( putAttemptedAndFailed ) {
					System.err.println("Okay, that failed PUT actually was a failure, since the file's not on the server.");
					anyFailures = true;
					// No point passing this on.
					return false;
				}
				
				return true;
			} else if( m instanceof FullyStoredMarker ) {
				// These are only 'guaranteed' (assuming the server's behaving) to be true if no errors occur.
				// So if errors have occurred, don't forward these!
				return !anyFailures;
			} else if( m instanceof LogMessage ) {
				// Ignored
				return false;
			} else if( m instanceof PutHead ) {
				// Ignored!
				// But it would be neat if there was a way to do this!
				// Maybe using PK crypto or something.
				// Feature for the future, maybe.
				return false;
			} else if( m instanceof EndMessage ) {
				completed = true;
				endReceived = true;
				keepGoing = false;
				// Caller will send an EndMessage onward
				// as part of its shutdown sequence.
				return false;
			} else {
				throw new RuntimeException("Unrecognized message: "+m.getClass());
			}
		}
		
		public void run() {
			try {
				while(keepGoing) {
					try {
						Object m = headTasks.take();
						if( handle(m) ) putTasks.put(m);
					} catch( InterruptedException e ) {
						keepGoing = false;
						this.interrupt();
					}
				}
				if( debug ) System.err.println(getName()+" exiting naturally.");
			} catch( Exception e ) {
				anyFailures = true;
				System.err.println(getName()+" exiting due to error");
				e.printStackTrace(System.err);
			} finally {
				try {
					putTasks.put(EndMessage.INSTANCE);
				} catch( InterruptedException e ) {
					this.interrupt();
				}
			}
			if( !endReceived && !interrupted() ) QueueDrainer.drain(headTasks);
		}
	};
	
	class PutThread extends WorkerThread {
		public PutThread( String name ) { super(name); }
		
		boolean endReceived = false;
		boolean keepGoing = true;
		
		/** Returns true to indicate that the message should be forwarded */
		protected void handle(Object m) throws Exception {
			if( debug ) System.err.println(getName()+" received "+m.getClass());
			if( m instanceof BlobInfo ) {
				try {
					upload((BlobInfo)m);
				} catch( ServerError e ) {
					anyFailures = true;
					reportServerError(e, true);
				}
			} else if( m instanceof FullyStoredMarker ) {
				if( !anyFailures ) fullyCachedUrnSet.add( ((FullyStoredMarker)m).urn );
			} else if( m instanceof LogMessage ) {
				// Ignored
			} else if( m instanceof PutHead ) {
				// Ignored
			} else if( m instanceof EndMessage ) {
				completed = true;
				endReceived = true;
				keepGoing = false;
			} else {
				throw new RuntimeException("Unrecognized message: "+m.getClass());
			}
		}
		
		public void run() {
			try {
				while(keepGoing) {
					try {
						Object m = putTasks.take();
						handle(m);
					} catch( InterruptedException e ) {
						keepGoing = false;
						this.interrupt();
					}
				}
				if( debug ) System.err.println(getName()+" exiting naturally.");
			} catch( Exception e ) {
				System.err.println(getName()+" exiting due to error");
				e.printStackTrace(System.err);
				anyFailures = true;
			}
			if( !endReceived && !interrupted() ) QueueDrainer.drain(putTasks);
		};
	};
	
	@Override public void give(Object value) throws Exception {
		headTasks.put(value);
	}
	
	@Override public String toString() {
		return getClass().getSimpleName()+" "+serverName+" ("+serverUrl+")";
	}
	
	@Override public boolean completedSuccessfully() {
		WorkerThread[] threads = {headThread, putThread};
		for( WorkerThread t : threads ) {
			if( !t.completedSuccessfully() ) return false;
		}
		return true;
	}
	
	@Override public void start() {
		headThread.start();
		putThread.start();
		if(debug) System.err.println(getClass().getSimpleName()+": HTTP HEAD and PUT threads started");
	}
	
	@Override public void halt() {
		if(debug) System.err.println(getClass().getSimpleName()+": Halting HEAD and PUT threads...");
		headThread.interrupt();
		putThread.interrupt();
	}
	
	@Override public void join() throws InterruptedException {
		headThread.join();
		putThread.join();
		if(debug) System.err.println(getClass().getSimpleName()+": HTTP HEAD and PUT threads completed");
	}
}
