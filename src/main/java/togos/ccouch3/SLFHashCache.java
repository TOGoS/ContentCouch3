package togos.ccouch3;

import java.io.File;
import java.io.IOException;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.ccouch3.slf.SimpleListFile2;

public class SLFHashCache implements HashCache
{
	final File fileUrnCacheFile;
	final File dirUrnCacheFile;
	SimpleListFile2 fileUrnCache;
	SimpleListFile2 dirUrnCache;
	
	public SLFHashCache( File cacheDir ) {
		this.fileUrnCacheFile = new File(cacheDir + "/file-urns.slf2");
		this.dirUrnCacheFile = new File(cacheDir + "/dir-urns.slf2");
	}
	
	protected synchronized SimpleListFile2 getFileUrnCache() {
		if( fileUrnCache == null ) {
			fileUrnCache = SimpleListFile2.mkSlf(fileUrnCacheFile);
		}
		return fileUrnCache;
	}
	
	protected synchronized SimpleListFile2 getDirUrnCache() {
		if( dirUrnCache == null ) {
			dirUrnCache = SimpleListFile2.mkSlf(dirUrnCacheFile);
		}
		return dirUrnCache;
	}
	
	protected SimpleListFile2 getUrnCache( File f ) {
		return f.isDirectory() ? getDirUrnCache() : getFileUrnCache(); 
	}
	
	protected ByteChunk fileIdChunk( File f ) throws IOException {
		String fileId = f.getCanonicalPath() + ";mtime=" + f.lastModified() + ";size=" + f.length();
		return BlobUtil.byteChunk(fileId);
	}
	
	@Override
	public void cacheFileUrn(File f, String urn) throws IOException {
		SimpleListFile2 c = getUrnCache(f);
		ByteChunk fileIdChunk = fileIdChunk(f);
		ByteChunk urnChunk = BlobUtil.byteChunk(urn);
		synchronized( c ) { c.put( fileIdChunk, urnChunk ); }
	}
	
	@Override
	public String getFileUrn(File f) throws Exception {
		SimpleListFile2 c = getUrnCache(f);
		ByteChunk fileIdChunk = fileIdChunk(f);
		ByteChunk urnChunk;
		synchronized( c ) { urnChunk = c.get( fileIdChunk ); }
		return urnChunk != null ? BlobUtil.string( urnChunk ) : null;
	}
}
