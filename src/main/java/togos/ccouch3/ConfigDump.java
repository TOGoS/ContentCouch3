package togos.ccouch3;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import togos.ccouch3.CCouchContext.RepoSpec;
import togos.ccouch3.proz.SystemContext;
import togos.ccouch3.util.ParseResult;
import togos.ccouch3.util.StreamUtil;

public class ConfigDump
{
	static String repoInfoAsString(RepoSpec rs) {
		if( rs == null ) return "(unspecified)";
		return rs.location + (rs.name == null ? "" : " ("+rs.name+")");
	}
	
	public static int main(CCouchContext ccCtx, List<String> args, SystemContext sysCtx) {
		while( !args.isEmpty() ) {
			ParseResult<List<String>, CCouchContext> pr = ccCtx.handleCommandLineOption(args);
			if( pr.remainingInput == args ) {
				StreamUtil.toPrintStream(sysCtx.getOutputStream(1))
					.println("Unrecognized argument: "+args.get(0));
				return 1;
			}
		}
		
		ccCtx = ccCtx.fixed();
		
		PrintStream out = StreamUtil.toPrintStream(sysCtx.getOutputStream(0));
		
		out.println("Primary local repo: "+repoInfoAsString(ccCtx.primaryRepo));
		for( File f : ccCtx.getRepoDirs() ) {
			out.println("Local repo: "+f);
		}
		for( RepoSpec rs : ccCtx.remoteRepos ) {
			out.println("Remote repo: "+repoInfoAsString(rs));
		}
		return 0;
	}
}
