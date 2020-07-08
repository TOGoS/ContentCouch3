package togos.ccouch3;

import static togos.ccouch3.util.RepoURLDefuzzer.defuzzRemoteRepoPrefix;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;

import togos.ccouch3.repo.Repository;
import togos.ccouch3.repo.SHA1FileRepository;

public class RepoConfig
{
	enum RepoType {
		FILESYSTEM,
		HTTP_N2R
	}
	
	static class RepoSpec {
		public final String name;
		public final RepoType type;
		/** File path or HTTP server prefix */
		public final String location;
		
		public RepoSpec( String name, RepoType type, String location ) {
			this.name = name;
			this.type = type;
			this.location = location;
		}
		
		protected String getDescription() {
			String desc = "RepoConfig";
			if( name != null ) desc += " '"+name+"'";
			if( location != null ) desc += " ("+location+")";
			return desc;
		}
		
		public File getDirectory() {
			String name = "Repo";
			if( this.name != null ) name += " '"+name+"'";
				
			if( this.type != RepoType.FILESYSTEM ) throw new RuntimeException(getDescription()+" is not a local filesystem repo");
			if( this.location == null ) throw new RuntimeException(getDescription()+" has no location specified"); 
			return new File(location);
		}
	}
	
	/** Repository into which to store stuff. */ 
	public RepoSpec primaryRepo;
	/** Name of sector into which to store stuff. */
	public String storeSector = "user";
	/**
	 * List of repositories in addition to the primary one
	 * that are considered local
	 * (if we're caching and they contain stuff, nothing needs to be done).
	 */
	public final ArrayList<RepoSpec> localRepos = new ArrayList<RepoSpec>();
	/**
	 * List of remote repositories.
	 */
	public final ArrayList<RepoSpec> remoteRepos = new ArrayList<RepoSpec>();
	
	//// A bunch of crap; plz replace
	
	protected static String resolveRepoDir(String path) {
		if( path.startsWith("~/") ) {
			return System.getProperty("user.home") + "/" + path.substring(2);
		} else {
			return path;
		}
	}
	
	protected static String getNonEmptyEnv(String name) {
		String v = System.getenv(name);
		if( v.length() == 0 ) return null;
		return v;
	}
	
	protected static RepoSpec getEnvSpecifiedRepository() {
		// 2020-07-07 I have decided that using a default (based on user.home) is PROBLEMATIC
		// after on at least a couple occasions running out of disk space
		// when ccouch cached things to the default location, when I didn't mean for it to!
		String dir = getNonEmptyEnv("ccouch_repo_dir");
		if( dir == null ) return null;
		return new RepoSpec( getNonEmptyEnv("ccouch_repo_name"), RepoType.FILESYSTEM, dir );
	}
	
	//// end crap to be replaced
	
	static String optArg(String opt, String explicit, Iterator<String> rest) {
		if( explicit != null ) return explicit;
		
		if( !rest.hasNext() ) {
			throw new RuntimeException(opt+" requires an argument but wasn't given one");
		}
		
		return rest.next();
	}
	
	public boolean parseCommandLineArg( String rawOpt, Iterator<String> moreArgs ) {
		String opt;
		String explicitArgument = null;
		if( rawOpt.startsWith("--") ) {
			final int equalIndex = rawOpt.indexOf("=");
			if( equalIndex != -1 ) {
				explicitArgument = rawOpt.substring(equalIndex + 1);
				rawOpt = rawOpt.substring(0, equalIndex);
			}
			opt = rawOpt.substring(2);
		} else if( rawOpt.startsWith("-") ) {
			opt = rawOpt.substring(1);
		} else {
			return false;
		}
		
		final String name;
		final int colonIdx = opt.indexOf(':');
		if( colonIdx != -1 ) {
			name = opt.substring(colonIdx+1);
			opt = opt.substring(0, colonIdx);
		} else {
			name = null;
		}
		
		if( "repo".equals(opt) ) {
			primaryRepo = new RepoSpec(name, RepoType.FILESYSTEM, resolveRepoDir(optArg(rawOpt, explicitArgument, moreArgs)));
			return true;
		} else if( "local-repo".equals(opt) ) {
			localRepos.add(new RepoSpec(opt.substring("-repo:".length()), RepoType.FILESYSTEM, resolveRepoDir(optArg(rawOpt, explicitArgument, moreArgs))));
			return true;
		} else if( "remote-repo".equals(opt) ) {
			localRepos.add(new RepoSpec(opt.substring("-repo:".length()), RepoType.HTTP_N2R, defuzzRemoteRepoPrefix(optArg(rawOpt, explicitArgument, moreArgs))));
			return true;
		} else if( "sector".equals(opt) ) {
			storeSector = optArg(rawOpt, explicitArgument, moreArgs);
			return true;
		}
		
		return false;
	}
	
	/** Add in default repositor[ies] and load pointed-to ones */
	public void fix() {
		if( primaryRepo == null ) {
			primaryRepo = getEnvSpecifiedRepository();
		}
		// TODO: read primaryRepo/remote-repos.lst, or whatever it's called,
		// or any other configuration based on reading files in .ccouch,
		// which maybe actually we don't want to do at all lol
		// (preferring environment variables instead)
	}
	
	public File getPrimaryRepoDir() {
		if( primaryRepo == null ) {
			throw new RuntimeException("No primary repository configured");
		}
		return new File(primaryRepo.location);
	}
	
	public Repository[] getLocalRepositories() {
		File[] dirs = getRepoDirs();
		Repository[] repos = new Repository[dirs.length];
		for( int i=0; i<dirs.length; ++i ) {
			repos[i] = new SHA1FileRepository(new File(dirs[i], "data"),
				// Only give it a storeSector if it's the primary repository
				primaryRepo != null && dirs[i].getPath().equals(primaryRepo.location) ?
					storeSector : null);
		}
		return repos;
	}
	
	public File[] getRepoDirs() {
		LinkedHashSet<String> paths = new LinkedHashSet<String>();
		if( primaryRepo != null ) paths.add(primaryRepo.location);
		for( RepoSpec rs : localRepos ) paths.add(rs.location);
		File[] repoDirs = new File[paths.size()];
		int i=0; for( String p : paths ) {
			repoDirs[i++] = new File(p);
		}
		return repoDirs;
	}
	
	public String[] getRemoteRepoUrls() {
		LinkedHashSet<String> urls = new LinkedHashSet<String>();
		for( RepoSpec rs : localRepos ) urls.add(rs.location);
		return urls.toArray(new String[urls.size()]);
	}

	public SHA1FileRepository getPrimaryRepository() {
		return new SHA1FileRepository(new File(getPrimaryRepoDir(), "data"), storeSector);
	}
}
