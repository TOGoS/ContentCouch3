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
	
	protected final ArrayBlockingQueue<Object> headTasks = new ArrayBlockingQueue<Object>(1024);
	protected final ArrayBlockingQueue<Object> putTasks = new ArrayBlockingQueue<Object>(1024);
	
	public boolean debug = false;
	protected final WorkerThread headThread, putThread;
	
	boolean indiceRandomHeadErrors = true;
	boolean induceRandomPutErrors = true;
	
	public HTTPUploadClient(
		String serverName, String serverUrl, AddableSet<String> uc, TransferTracker tt
	) {
		this.serverName = serverName;
		this.serverUrl = serverUrl;
		this.fullyCachedUrnSet = uc;
		this.transferTracker = tt;
		
		this.headThread = new HeadThread(serverName+" HEAD thread");
		this.putThread  = new PutThread(serverName+" PUT thread");
	}
	
	@Override public String getServerName() { return serverName; }
	
	protected URL urlFor(String urn) throws MalformedURLException {
		return new URL(serverUrl + urn);
	}
	
	protected boolean existsOnServer( String urn ) throws IOException {
		if( indiceRandomHeadErrors ) throw new ServerError("Not a real error ha ha h", 599, null);
		
		URL url = urlFor(urn);
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
			throw new RuntimeException("HEAD received unexpectd response code "+status+"; "+urlCon.getResponseMessage());
		}
	}
	
	static class ServerError extends IOException {
		private static final long serialVersionUID = 1L;
		
		public final int statusCode;
		public final byte[] data;
		public ServerError( String message, int statusCode, byte[] data ) {
			super(message);
			this.statusCode = statusCode;
			this.data = data;
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
			transferTracker.sendingFile( ((FileInfo)bi).path );
		}
		
		if( induceRandomPutErrors && Math.random() <= 1.0 ) {
			throw new ServerError("Not a real server error; yuk yuk.", 599, null);
		}
		
		InputStream is = bi.openInputStream();
		try {
			URL url = urlFor(bi.getUrn());
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
					status, errorText);
			}
		} finally {
			is.close();
		}
		transferTracker.transferred(bi.getSize(), 1, tag);
	}
	
	int maxUploadAttempts = 1;
	
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
				if( bi.getSize() < SMALL_BLOB_SIZE ) {
					upload(bi);
					return false;
				} else if( existsOnServer(bi.getUrn()) ) {
					return false;
				} else {
					return true;
				}
			} else if( m instanceof FullyStoredMarker ) {
				return true;
			} else if( m instanceof LogMessage ) {
				// Ignored
				return false;
			} else if( m instanceof PutHead ) {
				// Ignored
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
				upload((BlobInfo)m);
			} else if( m instanceof FullyStoredMarker ) {
				fullyCachedUrnSet.add( ((FullyStoredMarker)m).urn );
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
