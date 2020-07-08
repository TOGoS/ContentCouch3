package togos.ccouch3;

import java.io.File;
import java.util.Iterator;

import togos.ccouch3.RepoConfig.RepoSpec;

public class ConfigDump
{
	static String repoInfoAsString(RepoSpec rs) {
		if( rs == null ) return "(unspecified)";
		return rs.location + (rs.name == null ? "" : " ("+rs.name+")");
	}
	
	public static int main(CCouchCommandContext gOpts, Iterator<String> argi) {
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( gOpts.repoConfig.handleCommandLineOption(arg, argi) ) {
			} else {
				System.err.println("Unrecognized argument: "+arg);
				return 1;
			}
		}
		
		gOpts.repoConfig.fix();
		
		System.out.println("Primary local repo: "+repoInfoAsString(gOpts.repoConfig.primaryRepo));
		for( File f : gOpts.repoConfig.getRepoDirs() ) {
			System.out.println("Local repo: "+f);
		}
		for( RepoSpec rs : gOpts.repoConfig.remoteRepos ) {
			System.out.println("Remote repo: "+repoInfoAsString(rs));
		}
		return 0;
	}
}
