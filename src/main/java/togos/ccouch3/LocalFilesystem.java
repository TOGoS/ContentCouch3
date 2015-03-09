package togos.ccouch3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import togos.blob.ByteBlob;
import togos.blob.file.FileBlob;
import togos.ccouch3.util.StreamUtil;

public class LocalFilesystem implements Filesystem
{
	static class FileMetadata extends File implements Filesystem.FileMetadata {
		private static final long serialVersionUID = 1L;
		
		public FileMetadata(String path) {
			super(path);
		}
		
		public FSObjectType getFsObjectType() {
			if(this.isDirectory()) return FSObjectType.DIRECTORY;
			if(this.exists()) return FSObjectType.BLOB;
			throw new RuntimeException(this+" doesn't exist");
		};
		
		@Override public long getLastModificationTime() {
			return this.lastModified();
		}
		
		@Override public String[] getObviousUrns() {
			return new String[0];
		}
		
		@Override public long getSize() {
			return this.length();
		}
	}
	
	protected final String prefix;
	public LocalFilesystem( String prefix ) {
		this.prefix = prefix;
	}
	
	protected String fullPath(String path) {
		return prefix+path;
	}
	
	@Override public FileMetadata getMetadata(String path) {
		String fullPath = fullPath(path);
		FileMetadata fmd = new FileMetadata(fullPath);
		return fmd.exists() ? fmd : null;
	}

	@Override public ByteBlob getBlob(String path) {
		String fullPath = fullPath(path);
		FileBlob fb = new FileBlob(fullPath);
		return fb.exists() ? fb : null;
	}

	@Override public String[] listDir(String path) {
		String fullPath = fullPath(path);
		return new File(fullPath).list();
	}

	@Override public void putBlob(String path, ByteBlob data, long mtime) throws IOException {
		// May want to try alternate methods,
		// e.g. hardlinks or cp --reflinks, etc.
		String fullPath = fullPath(path);
		File dest = new File(fullPath);
		File dir = dest.getParentFile();
		if( dir == null ) dir = new File(".");
		InputStream is = data.openInputStream();
		try {
			if( !dir.exists() ) dir.mkdirs();
			File temp = File.createTempFile(".temp-"+dest.getName(), ".temp", dir);
			FileOutputStream fos = new FileOutputStream(temp);
			try {
				StreamUtil.copy(is, fos);
			} finally {
				fos.close();
			}
			temp.renameTo(dest);
			if( mtime != -1 ) dest.setLastModified(mtime);
		} finally {
			is.close();
		}
	}
}
