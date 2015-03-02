package togos.ccouch3;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FileInfo implements BlobInfo
{
	public final String path;
	public final String urn;
	public final FSObjectType fileType;
	public final long size;
	public final long mtime;
	
	public FileInfo( String path, String urn, FSObjectType fileType, long size, long mtime ) {
		this.path     = path;
		this.urn      = urn;
		this.fileType = fileType;
		this.size     = size;
		this.mtime    = mtime;
	}

	public FileInfo( FileInfo fileInfo ) {
		this( fileInfo.path, fileInfo.urn, fileInfo.fileType, fileInfo.size, fileInfo.mtime );
	}

	@Override public String getUrn() { return urn; }
	@Override public long getSize() { return size; }
	
	public InputStream openInputStream() throws FileNotFoundException {
		return new FileInputStream(path);
	}
}
