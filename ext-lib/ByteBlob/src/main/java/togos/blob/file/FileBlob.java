package togos.blob.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import togos.blob.ByteBlob;

public class FileBlob extends File implements ByteBlob
{
	private static final long serialVersionUID = 1L;
	
	public FileBlob(String path) { super(path); }
	public FileBlob(File file) { this(file.getPath()); }
	
	@Override public InputStream openInputStream() throws IOException {
		return new FileInputStream(this);
	}
	
	@Override public long getSize() {
		return length();
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
}
