package togos.ccouch3;

import java.io.File;
import java.util.Iterator;

import togos.ccouch3.RepoConfig.RepoSpec;

public class ConfigDump
{
	public static int main(Iterator<String> argi) {
		RepoConfig repoConfig = new RepoConfig();
		
		while( argi.hasNext() ) {
			String arg = argi.next();
			if( repoConfig.parseCommandLineArg(arg, argi) ) {
			} else {
				System.err.println("Unrecognized argument: "+arg);
				return 1;
			}
		}
		
		repoConfig.fix();
		
		System.out.println("Primary local repo: "+repoConfig.getPrimaryRepoDir());
		for( File f : repoConfig.getRepoDirs() ) {
			System.out.println("Local repo: "+f);
		}
		for( RepoSpec rs : repoConfig.remoteRepos ) {
			System.out.println("Remote repo: "+rs.location+(rs.name == null ? "" : " ("+rs.name+")"));
		}
		return 0;
	}
}
