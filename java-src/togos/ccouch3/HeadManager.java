package togos.ccouch3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;
import togos.ccouch3.util.FileUtil;

public class HeadManager
{
	public File headRoot;
	
	public HeadManager( File headRoot ) {
		this.headRoot = headRoot;
	}
	
	protected String cleanName( String name ) {
		if( name.startsWith("/") ) name = name.substring(1);
		if( name.endsWith("/") ) name = name.substring(0, name.length()-1);
		return name;
	}
	
	protected static ByteChunk read( File f ) throws IOException {
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
	
	protected static File tempFile( File f ) {
		String ext = ".temp-" + System.currentTimeMillis() + "-" + (new Random()).nextInt(Integer.MAX_VALUE);
		return new File( f + ext );
	}
	
	protected static void write( File f, ByteChunk c ) throws IOException {
		FileUtil.mkParentDirs( f );
		File tempFile = tempFile( f );
		FileOutputStream fos = new FileOutputStream( tempFile );
		try {
			fos.write( c.getBuffer(), c.getOffset(), c.getSize() );
			if( !tempFile.renameTo( f ) ) {
				throw new IOException("Failed to rename "+tempFile+" to "+f);
			}
		} finally {
			if( tempFile.exists() ) tempFile.delete();
		}
	}
	
	public boolean addHead( String name, ByteChunk data ) throws IOException {
		name = cleanName( name );
		File latest = getLatestFile( name );
		
		int newId;
		if( latest != null ) {
			ByteChunk existing = read( latest );
			if( data.equals(existing) ) return false;
			
			newId = Integer.parseInt( latest.getName() )+1;
		} else {
			newId = 1;
		}
		
		write( new File( headRoot + "/" + name + "/" + newId ), data );
		return true;
	}
	
	Pattern HEAD_NAME_PAT = Pattern.compile("\\d+");
	
	public File getLatestFile( String name ) throws IOException {
		name = cleanName( name );
		File dir = new File( headRoot + "/" + name );
		int maxId = 0;
		File maxFile = null;
		if( dir.exists() ) for( File f : dir.listFiles() ) {
			if( f.isFile() && HEAD_NAME_PAT.matcher(f.getName()).matches() ) {
				int id = Integer.parseInt(f.getName());
				if( id > maxId ) {
					maxId = id;
					maxFile = f;
				}
			}
		}
		return maxFile;
	}
	
	public ByteChunk getLatest( String name ) throws IOException {
		return read( getLatestFile(name) );
	}
}
