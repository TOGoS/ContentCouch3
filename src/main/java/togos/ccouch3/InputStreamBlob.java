package togos.ccouch3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import togos.blob.ByteBlob;
import togos.blob.util.BlobUtil;

public class InputStreamBlob implements ByteBlob
{
	protected final InputStream is;
	public InputStreamBlob(InputStream is) {
		this.is = is;
	}
	
	@Override public long getSize() {
		return -1;
	}
	
	protected boolean opened = false;
	
	@Override
	public synchronized InputStream openInputStream() throws IOException {
		if( opened ) {
			throw new IOException("Input stream already opened; can't re-open it");
		}
		opened = true;
		return is;
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
	
	@Override public void writeTo(OutputStream os) throws IOException {
		BlobUtil.pipe(openInputStream(), os);
	}
}
