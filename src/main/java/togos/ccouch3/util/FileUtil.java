package togos.ccouch3.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import togos.blob.ByteChunk;
import togos.blob.util.BlobUtil;
import togos.blob.util.SimpleByteChunk;

public class FileUtil
{
	private FileUtil() { }
	
	static final String[] IGNORE_FILENAMES = {
		"thumbs.db", "desktop.ini"
	};
	
	public static boolean shouldIgnore( File f ) {
		if( f.isHidden() || f.getName().startsWith(".") ) return true;
		String name = f.getName().toLowerCase();
		for( String ifn : IGNORE_FILENAMES ) if( ifn.equals(name) ) return true;
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
			fos.write( data, offset, length );
			if( !tempFile.renameTo( f ) ) {
				throw new IOException("Failed to rename "+tempFile+" to "+f);
			}
		} finally {
			if( tempFile.exists() ) tempFile.delete();
			fos.close();
		}
	}
	
	public static void writeAtomic( File f, byte[] data ) throws IOException {
		writeAtomic(f, data, 0, data.length);
	}
	
	public static void writeAtomic( File f, ByteChunk c ) throws IOException {
		writeAtomic( f, c.getBuffer(), c.getOffset(), BlobUtil.chunkLength(c) );
	}
}
