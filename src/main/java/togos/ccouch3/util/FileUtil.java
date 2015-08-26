package togos.ccouch3.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;
import togos.ccouch3.Glob;

public class FileUtil
{
	private FileUtil() { }
	
	static final String[] IGNORE_FILENAMES = {
		"thumbs.db", "desktop.ini",
	};
	
	public static final Glob DEFAULT_IGNORES = Glob.load(null, new String[] { ".*" }, null);
	
	public static boolean shouldIgnore( Glob ignores, File f ) {
		Boolean matchGlobs = Glob.anyMatch(ignores, f, null);
		if( matchGlobs != null ) return matchGlobs.booleanValue();
		
		// If the globs didn't explicitly say to keep or ignore it,
		// follow a few additional rules
		// (maybe eventually these can be encoded by Globs):
		
		if( f.isHidden() ) return true;
		
		String lowerCaseName = f.getName().toLowerCase();
		for( String ifn : IGNORE_FILENAMES ) if( ifn.equals(lowerCaseName) ) return true;
		
		return false;
	}
	
	public static void mkParentDirs( File f ) {
		File p = f.getParentFile();
		if( p == null ) return;
		if( p.exists() ) return;
		p.mkdirs();
	}
	
	public static void deltree( File f ) {
		if( f.isDirectory() ) {
			for( File s : f.listFiles() ) {
				if( "..".equals(s.getName()) || ".".equals(s.getName())) continue;
				deltree(s);
			}
		}
		f.delete();
	}
	
	public static ByteChunk read( File f ) throws IOException {
		if( f == null ) return null;
		
		long len = f.length();
		if( len > 65536 ) throw new IOException( "Head size is way too big (max is 65k): "+len);
		
		byte[] data = new byte[(int)len];
		FileInputStream fis = new FileInputStream(f);
		try {
			int z, r=0;
			while( (z=fis.read(data,r,(int)len-r)) > 0 ) r += z;
			if( r < len ) {
				throw new IOException( "Failed to read all data from "+f+" (read "+r+" / "+len+" bytes)");
			}
			return new SimpleByteChunk(data);
		} finally {
			fis.close();
		}
	}
	
	public static File tempFile( File f ) {
		String ext = ".temp-" + System.currentTimeMillis() + "-" + (new Random()).nextInt(Integer.MAX_VALUE);
		return new File( f + ext );
	}
	
	public static void writeAtomic( File f, byte[] data, int offset, int length ) throws IOException {
		FileUtil.mkParentDirs( f );
		File tempFile = tempFile( f );
		FileOutputStream fos = new FileOutputStream( tempFile );
		try {
			try {
				fos.write( data, offset, length );
			} finally {
				fos.close();
			}
			if( !tempFile.renameTo( f ) ) {
				throw new IOException("Failed to rename "+tempFile+" to "+f);
			}
		} finally {
			if( tempFile.exists() ) tempFile.delete();
		}
	}
	
	public static void writeAtomic( File f, byte[] data ) throws IOException {
		writeAtomic(f, data, 0, data.length);
	}
	
	public static void writeAtomic( File f, ByteChunk c ) throws IOException {
		writeAtomic( f, c.getBuffer(), c.getOffset(), BlobUtil.chunkLength(c) );
	}
}
