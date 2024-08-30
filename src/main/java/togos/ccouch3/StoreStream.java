package togos.ccouch3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import togos.blob.ByteBlob;
import togos.ccouch3.repo.Repository;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;

public class StoreStream
{
	public static int main(CCouchContext ctx, List<String> args) {
		boolean noMoreOptions = false;
		boolean verbose = false;
		ArrayList<String> inputPaths = new ArrayList<String>();
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( "-".equals(arg) || !arg.startsWith("-") || noMoreOptions ) {
				inputPaths.add(arg);
			} else if( "-v".equals(arg) ) {
				verbose = true;
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
		
		ctx = ctx.fixed();
		Repository repo = ctx.getPrimaryRepository();
		
		final BlobResolver argumentResolver = CCouch3Command.getCommandLineFileResolver(
			ctx.getLocalRepositories(),
			ctx.getRepoDirs()
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
