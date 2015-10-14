package togos.ccouch3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;
import togos.ccouch3.util.StreamUtil;

public class Copy
{
	public static String USAGE =
		"Usage: ccouch3 copy [-repo <path]* <source> <destination>";
	
	public static int main(Iterator<String> argi) {
		String fromName = null;
		String toName = null;
		ArrayList<String> repoPaths = new ArrayList<String>();
		
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-repo".equals(arg) ) {
				repoPaths.add(0, argi.next());
			} else if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE);
				return 0;
			} else if( !arg.startsWith("-") || "-".equals(arg) ) {
				if( fromName == null ) {
					fromName = arg;
				} else if( toName == null ) {
					toName = arg;
				} else {
					System.err.println("Error: You can only specify 2 files.  Gave: '"+fromName+"', '"+toName+"', '"+arg+"'");
					return 1;
				}
			} else {
				System.err.println("Error: Unrecognized argument: '"+arg+"'");
				return 1;
			}
		}
		
		if( fromName == null || toName == null ) {
			System.err.println("Error: You must specify both source and destination.");
			return 1;
		}
		// TODO: Clean this up.  Consolidate in CCouch3Command or something.
		repoPaths.add(CCouch3Command.getDefaultRepositoryDir().getPath());
		File[] repoDirs = new File[repoPaths.size()];
		for( int i=0; i<repoPaths.size(); ++i ) {
			repoDirs[i] = CCouch3Command.resolveRepoDir(repoPaths.get(i));
		}
		BlobResolver resolver = CCouch3Command.getCommandLineFileResolver(repoDirs);
		Filesystem destFs = new LocalFilesystem("");
		
		try {
			ByteBlob from;
			try {
				from = resolver.getBlob(fromName);
			} catch( FileNotFoundException e ) {
				System.err.println("Couldn't find input file '"+fromName+"': "+e.getMessage());
				return 1;
			}
			
			if( "-".equals(toName) ) {
				InputStream is = from.openInputStream();
				try {
					StreamUtil.copy(is, System.out);
				} finally {
					is.close();
				}
				return 0;
			} else {
				destFs.putBlob(toName, from, -1);
			}
		} catch( IOException e ) {
			e.printStackTrace();
			return 1;
		}
		
		return 0;
	}
}
