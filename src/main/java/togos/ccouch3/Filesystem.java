package togos.ccouch3;

import java.io.IOException;

import togos.blob.ByteBlob;

public interface Filesystem
{
	static interface FileMetadata {
		FSObjectType getFsObjectType();
		public long getLastModificationTime();
		public long getSize();
		public String[] getObviousUrns();
	}
	
	/** Returns null if the file doesn't exist */
	public FileMetadata getMetadata(String path) throws IOException;
	/** Returns null if the file doesn't exist */
	public ByteBlob getBlob(String path) throws IOException;
	/** Throws exception directory doesn't exist */
	public String[] listDir(String path) throws IOException;
	/**
	 * Should attempt to create any needed parent directories.
	 * If mtime is not -1, the Filesystem should try to set the last modification
	 * time of the new file to that value; being unable to is not an error.
	 * If the file cannot be written at all, an IOException must be thrown.
	 * */
	public void putBlob(String path, ByteBlob data, long mtime) throws IOException;
}
