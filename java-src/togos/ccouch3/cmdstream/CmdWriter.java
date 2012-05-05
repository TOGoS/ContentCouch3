package togos.ccouch3.cmdstream;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

public class CmdWriter implements Flushable, Closeable
{
	public static final String[] BYE_CMD = new String[]{ "bye" };
	
	public String debugPrefix = null;
	public boolean closing = false;
	
	OutputStream os;
	public CmdWriter( OutputStream os ) {
		this.os = os;
	}
	
	protected static String escape( String arg ) {
		// Quick-and-dirty approach for now:
		return arg.replace("%", "%25").replace(" ", "%20");
	}
	
	public static String encode( String[] cmd ) {
		StringBuilder sb = new StringBuilder();
		for( int i=0; i<cmd.length; ++i ) {
			sb.append( escape(cmd[i]) );
			if( i < cmd.length - 1 ) sb.append( ' ' );
		}
		return sb.toString();
	}
	
	public void writeCmd( String[] cmd ) throws IOException {
		if( debugPrefix != null ) System.err.println( debugPrefix + encode(cmd) );
		for( int i=0; i<cmd.length; ++i ) {
			os.write( escape(cmd[i]).getBytes() );
			os.write( i == cmd.length - 1 ? '\n' : ' ');
		}
		os.flush();
	}
	
	public void writeNewline() throws IOException {
		os.write( '\n' );
	}
	
	public void writeChunk( byte[] buf, int offset, int length ) throws IOException {
		if( length == 0 ) return; // No reason to write zero-length chunks, and they break the reading API.
		writeCmd( new String[]{ "chunk", String.valueOf(length) } );
		os.write( buf, offset, length );
		writeNewline();
	}
	
	public void endChunks() throws IOException {
		writeCmd( new String[]{ "end-chunks" } );
	}
	
	/**
	 * Write a message indicating you are done talking to the server;
	 * the server should finish whatever commands are queued up and then close the stream.
	 * You can call bye() more than once but you should not call anything else afterwards.
	 * @throws IOException
	 */
	public void bye() throws IOException {
		if( !closing ) writeCmd( BYE_CMD );
		closing = true;
	}
	
	public void flush() throws IOException {  os.flush();  }
	
	public void close() throws IOException {  os.close();  }
}
