package togos.ccouch3.cmdstream;

import java.io.IOException;
import java.io.InputStream;

public class CmdChunkInputStream extends InputStream
{
	boolean hitEndOfChunks;
	final CmdReader r;
	
	public CmdChunkInputStream( CmdReader r ) {
		this.r = r;
	}
	
	@Override
	public int read( byte[] buf, int off, int len ) throws IOException {
		int z = r.readChunk( buf, off, len );
		if( z == -1 ) hitEndOfChunks = true;
		return z;
	}
	
	@Override
	public int read() throws IOException {
		byte[] b = new byte[1];
		if( read( b ) > 0 ) {
			return b[0];
		} else {
			return -1;
		}
	}
	
	/**
	 * Always close the CmdChunkInputStream to ensure that the next r.readCmd works properly!
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		if( !hitEndOfChunks ) {
			byte[] buffer = new byte[4096];
			while( read(buffer) != -1 );
		}
	}
}
