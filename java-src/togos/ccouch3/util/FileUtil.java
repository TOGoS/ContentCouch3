package togos.ccouch3.util;

import java.io.File;

public class FileUtil
{
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
}
