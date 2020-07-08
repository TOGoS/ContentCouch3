package togos.ccouch3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.OutChecker.OnDirCollision;
import togos.ccouch3.OutChecker.OnFileCollision;
import togos.ccouch3.OutChecker.Run;
import togos.ccouch3.util.StreamUtil;

public class Copy
{
	public static String USAGE =
		"Usage: ccouch3 copy [-repo <path]* <source> <destination>";
	
	public static int main(CCouchCommandContext gOpts, Iterator<String> argi) {
		String fromName = null;
		String toName = null;
		OnDirCollision onDirCollision = OnDirCollision.ABORT;
		OnFileCollision onFileCollision = OnFileCollision.KEEP;
		
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( gOpts.repoConfig.handleCommandLineOption(arg, argi) ) {
			} else if( "-merge".equals(arg) ) {
				onDirCollision = OnDirCollision.MERGE;
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
		
		gOpts.repoConfig.fix();
		BlobResolver blobResolver = CCouch3Command.getCommandLineFileResolver(gOpts.repoConfig);
		ObjectResolver resolver = new ObjectResolver(blobResolver);
		
		Object from;
		try {
			from = resolver.get(fromName);
		} catch( Exception e ) {
			System.err.println("Couldn't find input object '"+fromName+"': "+e.getMessage());
			return 1;
		}
		
		if( "-".equals(toName) ) {
			if( !(from instanceof ByteBlob) ) {
				System.err.println(fromName+" is not a byte blob; can't copy it to standard output");
				return 1;
			}
			try {
				InputStream is = ((ByteBlob)from).openInputStream();
				try {
					StreamUtil.copy(is, System.out);
				} catch( IOException e ) {
					System.err.println(e.getMessage());
					return 1;
				} finally {
					StreamUtil.close(is);
				}
			} catch( IOException e ) {
				System.err.println(e.getMessage());
				return 1;
			}
			return 0;
		}
		
		OutChecker oc = new OutChecker(resolver, new LocalFilesystem(""));
		try {
			oc.checkOut(fromName, -1, toName, onDirCollision, onFileCollision, Run.DRY);
			oc.checkOut(fromName, -1, toName, onDirCollision, onFileCollision, Run.ACTUAL);
		} catch( Exception e ) {
			e.printStackTrace();
			return 1;
		}
		
		return 0;
	}
}
