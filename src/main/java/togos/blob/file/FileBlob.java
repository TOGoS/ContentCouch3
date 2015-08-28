package togos.blob.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import togos.blob.ByteBlob;
import togos.blob.InputStreamable;
import togos.blob.util.BlobUtil;

public class FileBlob extends File implements ByteBlob
{
	private static final long serialVersionUID = 1L;
	
	public FileBlob(String path) { super(path); }
	public FileBlob(File file) { this(file.getPath()); }
	public FileBlob(File parent, String path) { super(parent, path); }
	
	@Override public InputStream openInputStream() throws IOException {
		return new FileInputStream(this);
	}
	
	@Override public long getSize() {
		return length();
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
	
	@Override public void writeTo(OutputStream os) throws IOException {
		BlobUtil.pipe((InputStreamable)this, os);
	}
}
