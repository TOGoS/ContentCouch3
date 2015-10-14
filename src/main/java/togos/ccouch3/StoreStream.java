package togos.ccouch3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;

public class StoreStream
{
	public static int main(Iterator<String> argi) {
		boolean noMoreOptions = false;
		boolean verbose = false;
		String storeSector = "local";
		ArrayList<String> inputPaths = new ArrayList<String>();
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-".equals(arg) || !arg.startsWith("-") || noMoreOptions ) {
				inputPaths.add(arg);
			} else if( "-v".equals(arg) ) {
				verbose = true;
			} else if( "-sector".equals(arg) ) {
				storeSector = argi.next();
			} else if( "--".equals(arg) ) {
				noMoreOptions = true;
			} else {
				System.err.println("Unrecognized argument: "+arg);
				return 1;
			}
		}
		
		if( inputPaths.size() == 0 ) inputPaths.add("-");
		
		File repoDir = CCouch3Command.getDefaultRepositoryDir();
		Repository repo = new SHA1FileRepository(new File(repoDir,"data"), storeSector);
		
		final BlobResolver argumentResolver = CCouch3Command.getCommandLineFileResolver(
			new Repository[] { repo },
			new File[] { repoDir }
		);
		
		int errorCount = 0;
		int storeCount = 0;
		
		for( String inputPath : inputPaths ) {
			ByteBlob b;
			try {
				b = argumentResolver.getBlob(inputPath);
			} catch( FileNotFoundException e ) {
				System.err.println("Error: Could not find "+inputPath);
				++errorCount;
				continue;
			} catch( IOException e ) {
				System.err.println("Error while finding "+inputPath+": "+e.getMessage());
				++errorCount;
				continue;
			}
			try {
				String urn = repo.put(b.openInputStream());
				System.out.println(urn);
			} catch( Exception e ) {
				System.err.println("Error while storing "+inputPath+": "+e.getMessage());
				++errorCount;
				continue;
			}
			++storeCount;
		}
		if( verbose ) {
			System.err.println(storeCount+" objects stored.");
		}
		if( errorCount > 0 ) {
			System.err.println("There were errors.");
			return 1;
		}
		return 0;
	}
}
