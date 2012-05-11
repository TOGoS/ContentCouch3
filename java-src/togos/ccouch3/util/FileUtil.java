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
}
