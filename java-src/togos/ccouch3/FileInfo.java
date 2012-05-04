package togos.ccouch3;

public class FileInfo
{
	static final int FILETYPE_BLOB = 1;
	static final int FILETYPE_DIRECTORY = 2;
	
	public final String path;
	public final String urn;
	public final int fileType;
	public final long size;
	public final long mtime;
	
	public FileInfo( String path, String urn, int fileType, long size, long mtime ) {
		this.path     = path;
		this.urn      = urn;
		this.fileType = fileType;
		this.size     = size;
		this.mtime    = mtime;
	}

	public FileInfo( FileInfo fileInfo ) {
		this( fileInfo.path, fileInfo.urn, fileInfo.fileType, fileInfo.size, fileInfo.mtime );
	}
}
