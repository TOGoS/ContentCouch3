package togos.ccouch3.cmdstream;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

public class CmdWriter implements Flushable, Closeable
{
	OutputStream os;
	public CmdWriter( OutputStream os ) {
		this.os = os;
	}
	
	public void writeCmd( String[] cmd ) throws IOException {
		for( int i=0; i<cmd.length; ++i ) {
			os.write( cmd[i].getBytes() );
			os.write( i == cmd.length - 1 ? '\n' : ' ');
		}
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
	
	public void flush() throws IOException {  os.flush();  }
	
	public void close() throws IOException {  os.close();  }
}
