package togos.ccouch3.slf;


import java.io.File;

public class FileUtil
{
	public static void makeParentDirs( File f ) {
		File parentFile = f.getParentFile(); 
		if( parentFile != null && !parentFile.exists() ) {
			parentFile.mkdirs();
		}
	}
		
	public static void deltree( File f ) {
		if( f.isDirectory() ) {
			File[] list = f.listFiles();
			for( int i=0; i<list.length; ++i ) {
				deltree( list[i] );
			}
		} else if( f.exists() ) {
			f.delete();
		}
	}
	
}

