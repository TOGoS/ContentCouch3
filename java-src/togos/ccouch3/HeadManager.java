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
	
	public int addHead( String name, int minId, ByteChunk data ) throws IOException {
		name = cleanName( name );
		File latest = getLatestFile( name );
		
		int newId;
		if( latest != null ) {
			int latestId = Integer.parseInt( latest.getName() );
			
			// Always create a new head if min is greater than the latest ID.
			// Otherwise, only create a new one if it would be different
			// than the latest one.
			if( minId <= latestId ) {
				ByteChunk existing = FileUtil.read( latest );
				if( data.equals(existing) ) return latestId;
			}
			
			newId = latestId + 1;
		} else {
			newId = 1;
		}
		
		if( newId < minId ) newId = minId;
		
		FileUtil.writeAtomic( new File( headRoot + "/" + name + "/" + newId ), data );
		return newId;
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
	
	public int getLatestNumber( String name ) throws IOException {
		File f = getLatestFile(name);
		if( f == null ) return 0;
		return Integer.parseInt(f.getName());
	}
	
	public ByteChunk getLatest( String name ) throws IOException {
		return FileUtil.read( getLatestFile(name) );
	}
}
