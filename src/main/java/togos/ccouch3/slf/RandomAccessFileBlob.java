package togos.ccouch3.slf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;

public class RandomAccessFileBlob extends RandomAccessFile
	implements RandomAccessBlob
{
	protected final String filePath;
	
	public RandomAccessFileBlob( String arg0, String arg1 ) throws FileNotFoundException {
		super( arg0, arg1 );
		filePath = arg0;
	}
	
	public RandomAccessFileBlob( File arg0, String arg1 ) throws FileNotFoundException {
		super( arg0, arg1 );
		filePath = arg0.getPath();
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public long getSize() {
		try {
			return this.length();
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public void flush() {
		try {
			super.getChannel().force(true);
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public ByteChunk get( long offset, int length ) {
		try {
			seek( offset );
			byte[] buf = new byte[length];
			int z = 0;
			while( z < length ) {
				int r = read( buf, z, length-z );
				if( r == -1 ) break;
				z += r;
			}
			return length == z ?
				SimpleByteChunk.get(buf, 0, length) :
				SimpleByteChunk.copyOf(buf, 0, z);
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
	
	public void put( long offset, ByteChunk data ) {
		try {
			seek( offset );
			write( data.getBuffer(), data.getOffset(), BlobUtil.chunkLength(data) );
		} catch( IOException e ) {
			throw new RuntimeException(e);
		}
	}
}
