package togos.ccouch3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import togos.blob.ByteBlob;
import togos.ccouch3.util.ListUtil;
import togos.ccouch3.util.ParseResult;

public class Cat
{
	public static String USAGE =
		"Usage: ccouch3 cat [-repo <path>]* [<resource-path> ...]";
	
	public static int main(CCouchContext ctx, List<String> args) {
		ArrayList<String> resourcePaths = new ArrayList<String>();
		
		while( !args.isEmpty() ) {
			ParseResult<List<String>,CCouchContext> ctxPr = ctx.handleCommandLineOption(args);
			if( ctxPr.remainingInput != args ) {
				args = ctxPr.remainingInput;
				ctx  = ctxPr.result;
				continue;
			}
			
			String arg = ListUtil.car(args);
			args = ListUtil.cdr(args);
			if( CCouch3Command.isHelpArgument(arg) ) {
				System.out.println(USAGE);
				return 0;
			} else if( !arg.startsWith("-") || "-".equals(arg) ) {
				resourcePaths.add(arg);
			} else {
				System.err.println("Error: Unrecognized argument: '"+arg+"'");
				return 1;
			}
		}
		
		ctx = ctx.fixed();
		
		BlobResolver resolver = CCouch3Command.getCommandLineFileResolver(ctx);
		
		for( String path : resourcePaths ) {
			ByteBlob from;
			try {
				try {
					from = resolver.getBlob(path);
				} catch( FileNotFoundException e ) {
					System.err.println("Couldn't find input file '"+path+"': "+e.getMessage());
					return 1;
				}
				
				from.writeTo(System.out);
			} catch( IOException e ) {
				e.printStackTrace();
				return 1;
			}
		}
		return 0;
	}
}
