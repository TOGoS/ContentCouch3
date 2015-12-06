package togos.ccouch3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import togos.blob.ByteBlob;
import togos.blob.util.BlobUtil;

public class URLBlob implements ByteBlob
{
	public final URL url;
	public URLBlob(URL url) {
		this.url = url;
	}
	@Override public InputStream openInputStream() throws IOException {
		return url.openStream();
	}
	@Override public void writeTo(OutputStream os) throws IOException {
		BlobUtil.pipe(this, os);
	}
	@Override public long getSize() { return -1; }
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
}
