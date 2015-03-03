package togos.ccouch3;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import togos.ccouch3.FlowUploader.EndMessage;
import togos.ccouch3.FlowUploader.FileMissing;
import togos.ccouch3.FlowUploader.FullyStoredMarker;
import togos.ccouch3.FlowUploader.LogMessage;
import togos.ccouch3.FlowUploader.Piper;
import togos.ccouch3.FlowUploader.PutHead;
import togos.ccouch3.FlowUploader.Sink;
import togos.ccouch3.FlowUploader.TransferTracker;
import togos.ccouch3.cmdstream.CmdReader;
import togos.ccouch3.cmdstream.CmdWriter;
import togos.ccouch3.util.AddableSet;

class CommandUploadClient implements UploadClient
{
	static class HeadRequestSender implements Sink<Object> {
		protected final CmdWriter w;
		protected final TransferTracker tt;
		
		public HeadRequestSender( CmdWriter w, TransferTracker tt ) {
			this.w = w;
			this.tt = tt;
		}
		
		@Override
		public void give( Object m ) throws Exception {
			try {
				if( m instanceof FileInfo ) {
					FileInfo fi = (FileInfo)m;
					w.writeCmd( new String[]{ "head", fi.path, fi.urn } );
				} else if( m instanceof SmallBlobInfo ) {
					SmallBlobInfo bi = (SmallBlobInfo)m;
					w.writeCmd( new String[]{ "put", bi.urn, bi.urn, "chunk", String.valueOf(bi.getSize()) } );
					w.writeChunk( bi );
					w.endChunks();
					tt.transferred( bi.getSize(), 1, TransferTracker.TAG_TREENODE );
				} else if( m instanceof FullyStoredMarker ) {
					FullyStoredMarker fsm = (FullyStoredMarker)m;
					w.writeCmd( new String[]{ "echo", "fully-stored", fsm.urn} );
				} else if( m instanceof LogMessage ) {
					LogMessage lm = (LogMessage)m;
					w.writeCmd( new String[]{ "post", "x", "incoming-log", "chunk", String.valueOf(lm.message.length) } );
					w.writeChunk( lm.message, 0, lm.message.length );
					w.endChunks();
				} else if( m instanceof PutHead ) {
					PutHead ph = (PutHead)m;
					w.writeCmd( new String[]{ "put", "x", "ccouch-head:"+ph.name+"/"+ph.number, "by-urn", ph.headDataUrn } );
				} else if( m instanceof EndMessage ) {
					w.bye();
				} else {
					w.bye();
					throw new RuntimeException("Don't know what to do with "+m.getClass());
				}
			} finally {
				w.flush();
			}
		}
	}
	
	static class Uploader implements Sink<Object> {
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
				try {
					for( int z = fis.read(buffer); z >= 0; z = fis.read(buffer) ) {
						w.writeChunk( buffer, 0, z );
						tt.transferred( z, 0, TransferTracker.TAG_FILE );
					}
				} finally {
					fis.close();
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
				w.flush();
				throw new RuntimeException("Unexpected message: "+m.toString());
			}
			w.flush();
		}
	}
	
	static class CmdResponseReader implements Runnable, Closeable {
		protected final CmdReader r;
		protected final Sink<Object> messageSink;
		protected boolean closing = false;
		
		public CmdResponseReader( CmdReader r, Sink<Object> messageSink ) {
			this.r = r;
			this.messageSink = messageSink;
		}
		
		public void close() throws IOException {
			synchronized( this ) {
				closing = true;
			}
			r.close();
		}
		
		protected String[] readCmd() throws IOException {
			// Sometimes if underlying streams get closed the 'wrong way'
			// (e.g. by killing a process that you're reading from) read() returns
			// IOExceptions.  To prevent these from propagating to the rest of
			// the program, use CmdResponseReader#close() before killing the command
			// process.  Then if an exception is caught, here, we just return null to
			// indicate 'no more messages'.
			try {
				return r.readCmd();
			} catch( IOException e ) {
				synchronized( this ) {
					if( !closing ) throw e;
				}
				return null;
			}
		}
		
		public void run() {
			String[] m;
			try {
				while( (m = readCmd()) != null ) {
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
						} else if( "post".equals(m[1]) ) {
							// Great!
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
	
	public final String serverName;
	public final String[] serverCommand;
	public boolean debug = false;
	public boolean dieWhenNothingToSend = false;
	protected final TransferTracker transferTracker;
	protected final AddableSet<String> uploadCache;
	
	protected boolean started;
	protected boolean anythingSent;
	Process headProc, uploadProc;
	CommandUploadClient.HeadRequestSender headRequestSender;
	CommandUploadClient.CmdResponseReader headResponseReader;
	Piper headErrorPiper;
	Thread headResponseReaderThread;
	CommandUploadClient.Uploader uploader;
	CommandUploadClient.CmdResponseReader uploadResponseReader;
	Thread uploadResponseReaderThread;
	Piper uploadErrorPiper;
	
	/** The exit code that gets reported for processes that we didn't actually see finish */
	public static final int INCOMPLETE_EXIT_CODE = -1336;
	
	public int headProcExitCode   = INCOMPLETE_EXIT_CODE;
	public int uploadProcExitCode = INCOMPLETE_EXIT_CODE;
	
	public CommandUploadClient( String serverName, String[] serverCommand, AddableSet<String> uc, TransferTracker tt ) {
		this.serverName = serverName;
		this.serverCommand = serverCommand;
		this.transferTracker = tt;
		this.uploadCache = uc;
	}
	
	@Override public String getServerName() { return serverName; }
	
	@Override public String toString() {
		return getClass().getSimpleName()+" "+serverName+" ("+serverCommand[0]+" ...)";
	}
	
	public void give( Object m ) throws Exception {
		if( !anythingSent && m instanceof EndMessage && dieWhenNothingToSend ) {
			// Then we can quit without waiting for the server to
			// forward our EndMessages back to us!
			if( debug ) System.err.println(serverName + " uploader quitting early!");
			halt();
		} else {
			if( headRequestSender == null ) {
				throw new RuntimeException(serverName+" CmdResponseReader has not been started; cannot write to it!");
			}
			
			headRequestSender.give(m);
			anythingSent = true;
		}
	}
	
	public void start() {
		synchronized( this ) {
			if( started ) return;
			started = true;
		}
		
		try {
			headProc = Runtime.getRuntime().exec(serverCommand);
			uploadProc = Runtime.getRuntime().exec(serverCommand);
		} catch( IOException e ) {
			throw new RuntimeException("Failed to start '"+serverName+"' command server process", e);
		}
		
		headErrorPiper = new Piper( headProc.getErrorStream(), System.err );
		uploadErrorPiper = new Piper( uploadProc.getErrorStream(), System.err );
		
		uploader = new Uploader( new CmdWriter(uploadProc.getOutputStream()), transferTracker );
		headRequestSender = new HeadRequestSender( new CmdWriter(headProc.getOutputStream()), transferTracker );
		headResponseReader = new CmdResponseReader(
			new CmdReader(headProc.getInputStream()),
			uploader
		);
		uploadResponseReader = new CmdResponseReader(
			new CmdReader(uploadProc.getInputStream()),
			new Sink<Object>() { public void give(Object value) throws Exception {
				if( value instanceof FullyStoredMarker ) {
					FullyStoredMarker fsm = (FullyStoredMarker)value;
					uploadCache.add( fsm.urn );
				}
			} }
		);
		
		if( debug ) {
			uploader.w.debugPrefix = "Send upload command to "+serverName+": ";
			headRequestSender.w.debugPrefix = "Send head command to "+serverName+": ";
			headResponseReader.r.debugPrefix = "Read head response from "+serverName+": ";
			uploadResponseReader.r.debugPrefix = "Read upload response from "+serverName+": ";
		}
		
		headResponseReaderThread = new Thread( headResponseReader, "Head Response Reader" );
		uploadResponseReaderThread = new Thread( uploadResponseReader, "Upload Response Reader" );
		
		headErrorPiper.start();
		uploadErrorPiper.start();
		headResponseReaderThread.start();
		uploadResponseReaderThread.start();
	}
	
	protected static void close( Closeable c, String description ) {
		try {
			if( c != null ) c.close();
		} catch( IOException e ) {
			System.err.println("Error closing "+description+":\n");
			e.printStackTrace(System.err);
		}
	}
	
	public void halt() {
		// Close the processes so that read threads will return immediately
		close( headResponseReader, "head response reader" );
		close( uploadResponseReader, "upload response reader" );
		if( headProc != null ) headProc.destroy();
		if( uploadProc != null ) uploadProc.destroy();
		if( headErrorPiper != null ) headErrorPiper.interrupt();
		if( uploadErrorPiper != null ) uploadErrorPiper.interrupt();
	}
	
	public void join() throws InterruptedException {
		headProcExitCode = headProc.waitFor();
		uploadProcExitCode = uploadProc.waitFor();
		headResponseReaderThread.join();
		uploadResponseReaderThread.join();
		headErrorPiper.join();
		uploadErrorPiper.join();
	}
	
	@Override public boolean completedSuccessfully() {
		return headProcExitCode == 0 && uploadProcExitCode == 0;
	}
}
