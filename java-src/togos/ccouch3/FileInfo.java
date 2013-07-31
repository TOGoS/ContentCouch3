package togos.ccouch3;

public class FileInfo
{
	public enum FileType {
		BLOB("Blob"),
		DIRECTORY("Directory");

		public final String niceName;
		FileType( String niceName ) {
			this.niceName = niceName;
		}
	};
	
	public final String path;
	public final String urn;
	public final FileType fileType;
	public final long size;
	public final long mtime;
	
	public FileInfo( String path, String urn, FileType fileType, long size, long mtime ) {
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
