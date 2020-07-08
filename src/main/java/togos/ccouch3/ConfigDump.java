package togos.ccouch3;

import java.io.File;
import java.util.Iterator;

import togos.ccouch3.CCouchContext.RepoSpec;

public class ConfigDump
{
	static String repoInfoAsString(RepoSpec rs) {
		if( rs == null ) return "(unspecified)";
		return rs.location + (rs.name == null ? "" : " ("+rs.name+")");
	}
	
	public static int main(CCouchContext ctx, Iterator<String> argi) {
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( ctx.handleCommandLineOption(arg, argi) ) {
			} else {
				System.err.println("Unrecognized argument: "+arg);
				return 1;
			}
		}
		
		ctx.fix();
		
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
