package togos.ccouch3;

import java.io.File;
import java.util.List;

import togos.ccouch3.CCouchContext.RepoSpec;
import togos.ccouch3.util.ParseResult;

public class ConfigDump
{
	static String repoInfoAsString(RepoSpec rs) {
		if( rs == null ) return "(unspecified)";
		return rs.location + (rs.name == null ? "" : " ("+rs.name+")");
	}
	
	public static int main(CCouchContext ctx, List<String> args) {
		while( !args.isEmpty() ) {
			ParseResult<List<String>, CCouchContext> pr = ctx.handleCommandLineOption(args);
			if( pr.remainingInput == args ) {
				System.err.println("Unrecognized argument: "+args.get(0));
				return 1;
			}
		}
		
		ctx = ctx.fixed();
		
		System.out.println("Primary local repo: "+repoInfoAsString(ctx.primaryRepo));
		for( File f : ctx.getRepoDirs() ) {
			System.out.println("Local repo: "+f);
		}
		for( RepoSpec rs : ctx.remoteRepos ) {
			System.out.println("Remote repo: "+repoInfoAsString(rs));
		}
		return 0;
	}
}
