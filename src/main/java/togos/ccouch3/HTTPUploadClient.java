package togos.ccouch3;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;

import togos.ccouch3.FlowUploader.EndMessage;
import togos.ccouch3.FlowUploader.FullyStoredMarker;
import togos.ccouch3.FlowUploader.LogMessage;
import togos.ccouch3.FlowUploader.PutHead;
import togos.ccouch3.FlowUploader.TransferTracker;
import togos.ccouch3.util.AddableSet;

class HTTPUploadClient implements UploadClient
{
	protected final String name;
	protected final String repoUrl;
	protected final AddableSet<String> fullyCachedUrnSet;
	protected final TransferTracker transferTracker;
	
	protected final ArrayBlockingQueue<Object> headTasks = new ArrayBlockingQueue<Object>(1024);
	protected final ArrayBlockingQueue<Object> putTasks = new ArrayBlockingQueue<Object>(1024);
	
	protected URL urlFor(String urn) throws MalformedURLException {
		return new URL(repoUrl + urn);
	}
	
	protected boolean existsOnServer( String urn ) throws IOException {
		URL url = urlFor(urn);
		HttpURLConnection urlCon = (HttpURLConnection)url.openConnection();
		urlCon.setRequestMethod("HEAD");
		urlCon.connect();
		int status = urlCon.getResponseCode();
		if( status == 200 ) {
			return true;
		} else if( status == 404 ) {
			return false;
		} else {
			throw new RuntimeException("HEAD received unexpectd response code "+status);
		}
	}
	
	protected void upload( BlobInfo bi ) {
		throw new UnsupportedOperationException();
	}
	
	final int SMALL_BLOB_SIZE = 8192;
	
	Thread headThread = new Thread() {
		protected boolean keepGoing = true;
		
		/** Returns true to indicate that the message should be forwarded */
		protected boolean handle(Object m) throws Exception {
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
				keepGoing = false;
				// Caller will send an EndMessage onward
				// as part of its shutdown sequence.
				return true;
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
					} catch( Exception e ) {
						throw new RuntimeException(e);
					}
				}
			} finally {
				try {
					putTasks.put(EndMessage.INSTANCE);
				} catch( InterruptedException e ) {
					this.interrupt();
				}
			}
		}
	};
	
	Thread putThread = new Thread() {
		boolean keepGoing = true;
		
		/** Returns true to indicate that the message should be forwarded */
		protected void handle(Object m) throws Exception {
			if( m instanceof BlobInfo ) {
				upload((BlobInfo)m);
			} else if( m instanceof FullyStoredMarker ) {
				fullyCachedUrnSet.add( ((FullyStoredMarker)m).urn );
			} else if( m instanceof LogMessage ) {
				// Ignored
			} else if( m instanceof PutHead ) {
				// Ignored
			} else if( m instanceof EndMessage ) {
				keepGoing = false;
			} else {
				throw new RuntimeException("Unrecognized message: "+m.getClass());
			}
		}
		
		public void run() {
			while(keepGoing) {
				try {
					Object m = headTasks.take();
					handle(m);
				} catch( InterruptedException e ) {
					keepGoing = false;
					this.interrupt();
				} catch( Exception e ) {
					throw new RuntimeException(e);
				}
			}
		};
	};
	
	public HTTPUploadClient(
		String name, String url, AddableSet<String> uc, TransferTracker tt
	) {
		this.name = name;
		this.repoUrl = url;
		this.fullyCachedUrnSet = uc;
		this.transferTracker = tt;
	}
	
	@Override public void give(Object value) throws Exception {
		headTasks.put(value);
	}
	
	@Override public void start() {
		headThread.start();
		putThread.start();
	}
	
	@Override public void halt() {
		headThread.interrupt();
		putThread.interrupt();
	}
	
	@Override public void join() throws InterruptedException {
		headThread.join();
		putThread.join();
	}
}
