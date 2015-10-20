package togos.ccouch3;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.ccouch3.Filesystem.FileMetadata;

public class FileInfo extends FileBlob implements BlobInfo, FileMetadata
{
	private static final long serialVersionUID = 1L;
	
	private final String urn;
	private final long size;
	private final FSObjectType fsObjectType;
	private final long mtime;
	
	public FileInfo( String path, String urn, FSObjectType fileType, long size, long mtime ) {
		super(path);
		this.urn      = urn;
		this.fsObjectType = fileType;
		this.size     = size;
		this.mtime    = mtime;
	}

	public FileInfo( FileInfo fileInfo ) {
		this( fileInfo.getPath(), fileInfo.urn, fileInfo.fsObjectType, fileInfo.size, fileInfo.mtime );
	}
	
	@Override public String getUrn() { return urn; }
	/** May return -1 to indicate unknown */
	@Override public long getSize() { return size; }
	
	public InputStream openInputStream() throws FileNotFoundException {
		return new FileInputStream(this);
	}
	
	@Override public ByteBlob slice(long offset, long length) {
		throw new UnsupportedOperationException();
	}
	
	@Override public FSObjectType getFsObjectType() { return fsObjectType; }
	/** May return -1 to indicate unknown */
	@Override public long getLastModificationTime() { return mtime; }
	@Override public String[] getObviousUrns() { return new String[] { urn }; };
}
