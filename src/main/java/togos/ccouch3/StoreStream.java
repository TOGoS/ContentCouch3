package togos.ccouch3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import togos.blob.ByteBlob;
import togos.ccouch3.CCouch3Command.GeneralOptions;
import togos.ccouch3.repo.Repository;

public class StoreStream
{
	public static int main(GeneralOptions gOpts, Iterator<String> argi) {
		boolean noMoreOptions = false;
		boolean verbose = false;
		ArrayList<String> inputPaths = new ArrayList<String>();
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( "-".equals(arg) || !arg.startsWith("-") || noMoreOptions ) {
				inputPaths.add(arg);
			} else if( "-v".equals(arg) ) {
				verbose = true;
			} else if( gOpts.repoConfig.parseCommandLineArg(arg, argi)) { 
			} else if( "--".equals(arg) ) {
				noMoreOptions = true;
			} else {
				System.err.println("Unrecognized argument: "+arg);
				return 1;
			}
		}
		
		if( inputPaths.size() == 0 ) {
			System.err.println("Warning: No inputs given; defaulting to '-'; this behavior may change");
			inputPaths.add("-");
		}
		
		gOpts.repoConfig.fix();
		Repository repo = gOpts.repoConfig.getPrimaryRepository();
		
		final BlobResolver argumentResolver = CCouch3Command.getCommandLineFileResolver(
			gOpts.repoConfig.getLocalRepositories(),
			gOpts.repoConfig.getRepoDirs()
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
