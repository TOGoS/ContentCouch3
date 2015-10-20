package togos.ccouch3;

import java.io.IOException;

import togos.blob.ByteBlob;
import togos.ccouch3.Filesystem.FileMetadata;

public class OutChecker
{
	static class CollisionException extends Exception {
		private static final long serialVersionUID = 1L;
		
		public CollisionException( String message ) {
			super(message);
		}
	}
	
	enum OnDirCollision {
		ABORT,
		MERGE
	}
	
	enum OnFileCollision {
		ABORT,
		ENSURE_SAME,
		KEEP,
		OVERWRITE
	}
	
	enum Run {
		DRY,
		ACTUAL
	}
	
	protected final ObjectResolver resolver;
	protected final Filesystem filesystem;
	
	public OutChecker( ObjectResolver resolver, Filesystem filesystem ) {
		this.resolver   = resolver;
		this.filesystem = filesystem;
	}
	
	public void checkOutFile( ByteBlob data, long mtime, String path, OnFileCollision onFileCollision, Run run )
		throws CollisionException, IOException
	{
		FileMetadata md = filesystem.getMetadata(path);
		if( md != null ) {
			switch( onFileCollision ) {
			case ABORT: throw new CollisionException(path+" already exists");
			case KEEP: return;
			case OVERWRITE: break;
			case ENSURE_SAME:
				throw new UnsupportedOperationException("ENSURE_SAME not yet supported");
			}
		}
		if( md != null && md.getFsObjectType() != FSObjectType.BLOB ) {
			throw new CollisionException(path+" is not a blob, but you're trying to overwrite it with one");
		}
		if( run == Run.ACTUAL ) {
			filesystem.putBlob(path, data, mtime);
		}
	}
	
	public void checkOutDir( Directory dir, String path, OnDirCollision onDirCollision, OnFileCollision onFileCollision, Run run )
		throws Exception
	{
		FileMetadata md = filesystem.getMetadata(path);
		if( md != null ) {
			switch( onDirCollision ) {
			case MERGE: break; // We'll make sure it's a directory in a bit.
			case ABORT: throw new CollisionException(path+" already exists");
			}
		}
		if( md != null && md.getFsObjectType() != FSObjectType.DIRECTORY ) {
			throw new CollisionException(path+" is not a directory, but you're trying to overwrite it with one");
		}
		if( run == Run.ACTUAL ) {
			filesystem.mkdir(path);
		}
		for( DirectoryEntry e : dir ) {
			checkOut( e.getUrn(), e.getLastModificationTime(), path+"/"+e.name, onDirCollision, onFileCollision, run );
		}
	}
	
	public void checkOut( String uri, long mtime, String path, OnDirCollision onDirCollision, OnFileCollision onFileCollision, Run run )
		throws Exception
	{
		Object obj = resolver.get(uri);
		if( obj instanceof ByteBlob ) {
			checkOutFile( (ByteBlob)obj, mtime, path, onFileCollision, run );
		} else if( obj instanceof Directory ) {
			checkOutDir( (Directory)obj, path, onDirCollision, onFileCollision, run );
		} else if( obj instanceof Commit ) {
			checkOut( ((Commit)obj).targetUrn, mtime, path, onDirCollision, onFileCollision, run );
		}
	}
}
